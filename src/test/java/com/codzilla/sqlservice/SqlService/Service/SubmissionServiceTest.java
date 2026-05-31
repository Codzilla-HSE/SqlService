package com.codzilla.sqlservice.SqlService.Service;

import com.codzilla.sqlservice.SqlService.DB.*;
import com.codzilla.sqlservice.SqlService.Dto.SubmissionStatusDto;
import com.codzilla.sqlservice.SqlService.kafka.KafkaConfig;
import com.codzilla.sqlservice.SqlService.Dto.SubmissionKafkaMessage;
import com.codzilla.sqlservice.SqlService.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.BDDMockito.*;

@ExtendWith(MockitoExtension.class)
class SubmissionServiceTest {

    @Mock private SubmissionRepository submissionRepository;
    @Mock private TaskRepository taskRepository;
    @Mock private KafkaTemplate<String, SubmissionKafkaMessage> kafkaTemplate;
    @Mock private SqlSecurityValidator securityValidator;

    @InjectMocks
    private SubmissionService submissionService;

    private Task dqlTask;
    private UUID userId = UUID.randomUUID();
    private String rawSql = "SELECT * FROM users";
    private String normalizedSql = "SELECT * FROM users";

    @BeforeEach
    void setUp() {
        dqlTask = Task.builder()
                .taskId(1L).type(TaskType.DQL).correctSqlResponse("SELECT * FROM users")
                .initScriptKey("tasks/1/init.sql").timeLimitMs(5000).build();

        // Нормализация может не вызываться в тестах с ошибкой валидации – делаем lenient
        lenient().when(securityValidator.normalize(anyString())).thenReturn(normalizedSql);
    }

    private void mockTaskFound() {
        given(taskRepository.findById(1L)).willReturn(Optional.of(dqlTask));
    }

    @Test
    void submit_validRequest_returnsSubmissionIdAndSavesPending() {
        mockTaskFound();
        // Успешная валидация
        given(securityValidator.validateUserSql(anyString(), eq(TaskType.DQL)))
                .willReturn(new SqlSecurityValidator.ValidationResult(true, null, null));

        // Имитация успешной отправки в Kafka
        given(kafkaTemplate.send(eq(KafkaConfig.SUBMISSION_TOPIC), eq("1"), any()))
                .willReturn(CompletableFuture.completedFuture(null));

        // При сохранении возвращаем объект с присвоенным ID
        given(submissionRepository.save(any(SqlSubmission.class))).willAnswer(inv -> {
            SqlSubmission s = inv.getArgument(0);
            s.setSubmissionId(100L);
            return s;
        });

        Long id = submissionService.submit(1L, userId, rawSql);

        assertThat(id).isEqualTo(100L);

        // Проверяем, что сохранили посылку в статусе PENDING
        ArgumentCaptor<SqlSubmission> captor = ArgumentCaptor.forClass(SqlSubmission.class);
        then(submissionRepository).should().save(captor.capture());
        SqlSubmission saved = captor.getValue();
        assertThat(saved.getStatus()).isEqualTo(SubmissionStatus.PENDING);
        assertThat(saved.getUserSqlQuery()).isEqualTo(normalizedSql);
        assertThat(saved.getUserId()).isEqualTo(userId);
        assertThat(saved.getTask()).isEqualTo(dqlTask);

        // Проверяем, что сообщение отправили в Kafka
        then(kafkaTemplate).should().send(eq(KafkaConfig.SUBMISSION_TOPIC), eq("1"), any(SubmissionKafkaMessage.class));
    }

    @Test
    void submit_invalidSql_throwsIllegalArgumentException() {
        mockTaskFound();
        // Валидация не проходит для любого SQL
        given(securityValidator.validateUserSql(anyString(), eq(TaskType.DQL)))
                .willReturn(new SqlSecurityValidator.ValidationResult(false, SqlVerdict.SECURITY_VIOLATION, "blocked"));

        assertThatThrownBy(() -> submissionService.submit(1L, userId, rawSql))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SECURITY_VIOLATION")
                .hasMessageContaining("blocked");

        // Сохранение не должно вызываться
        then(submissionRepository).should(never()).save(any());
        then(kafkaTemplate).should(never()).send(anyString(), anyString(), any());
    }

    @Test
    void submit_taskNotFound_throwsException() {
        given(taskRepository.findById(99L)).willReturn(Optional.empty());

        assertThatThrownBy(() -> submissionService.submit(99L, userId, "SELECT 1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Task not found");
    }

    @Test
    void getStatus_existingId_returnsDto() {
        SqlSubmission sub = SqlSubmission.builder()
                .submissionId(1L).status(SubmissionStatus.DONE).verdict(SqlVerdict.ACCEPTED)
                .executionTimeMs(120L).errorMessage(null).kafkaOffset(5L).build();
        given(submissionRepository.findById(1L)).willReturn(Optional.of(sub));

        SubmissionStatusDto dto = submissionService.getStatus(1L);

        assertThat(dto.submissionId()).isEqualTo(1L);
        assertThat(dto.status()).isEqualTo(SubmissionStatus.DONE);
        assertThat(dto.verdict()).isEqualTo(SqlVerdict.ACCEPTED);
        assertThat(dto.executionTimeMs()).isEqualTo(120L);
        assertThat(dto.kafkaOffset()).isEqualTo(5L);
    }

    @Test
    void getStatus_nonExistingId_throwsException() {
        given(submissionRepository.findById(2L)).willReturn(Optional.empty());

        assertThatThrownBy(() -> submissionService.getStatus(2L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Submission not found");
    }

    @Test
    void submit_kafkaFailure_updatesSubmissionToError() {
        mockTaskFound();
        // Успешная валидация
        given(securityValidator.validateUserSql(anyString(), eq(TaskType.DQL)))
                .willReturn(new SqlSecurityValidator.ValidationResult(true, null, null));

        // Имитация ошибки Kafka
        CompletableFuture<SendResult<String, SubmissionKafkaMessage>> failedFuture =
                CompletableFuture.failedFuture(new RuntimeException("Kafka down"));
        given(kafkaTemplate.send(eq(KafkaConfig.SUBMISSION_TOPIC), eq("1"), any()))
                .willReturn(failedFuture);

        // Сохраняем посылку, запоминаем её
        SqlSubmission[] savedHolder = new SqlSubmission[1];
        given(submissionRepository.save(any(SqlSubmission.class))).willAnswer(inv -> {
            SqlSubmission s = inv.getArgument(0);
            s.setSubmissionId(200L);
            savedHolder[0] = s;
            return s;
        });

        // Для markKafkaFailure нужно, чтобы findById вернул ту же посылку
        given(submissionRepository.findById(200L)).willAnswer(inv -> Optional.ofNullable(savedHolder[0]));

        Long id = submissionService.submit(1L, userId, rawSql);
        assertThat(id).isEqualTo(200L);

        // Проверяем, что статус изменился на ERROR
        ArgumentCaptor<SqlSubmission> captor = ArgumentCaptor.forClass(SqlSubmission.class);
        then(submissionRepository).should(atLeastOnce()).save(captor.capture());
        SqlSubmission updated = captor.getValue();
        assertThat(updated.getStatus()).isEqualTo(SubmissionStatus.ERROR);
        assertThat(updated.getVerdict()).isEqualTo(SqlVerdict.SYSTEM_ERROR);
        assertThat(updated.getErrorMessage()).contains("Kafka unavailable");
    }
}