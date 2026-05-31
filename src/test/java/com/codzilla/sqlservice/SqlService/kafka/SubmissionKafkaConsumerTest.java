package com.codzilla.sqlservice.SqlService.kafka;

import com.codzilla.sqlservice.SqlService.DB.*;
import com.codzilla.sqlservice.SqlService.Service.H2ExecutionService;
import com.codzilla.sqlservice.SqlService.Service.MinioService;
import com.codzilla.sqlservice.SqlService.Service.SqlSecurityValidator;
import com.codzilla.sqlservice.SqlService.Service.ValidatorScriptRunner;
import com.codzilla.sqlservice.SqlService.model.*;
import com.codzilla.sqlservice.SqlService.Dto.SubmissionKafkaMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.BDDMockito.*;

@ExtendWith(MockitoExtension.class)
class SubmissionKafkaConsumerTest {

    @Mock private SubmissionRepository submissionRepository;
    @Mock private TaskRepository taskRepository;
    @Mock private MinioService minioService;
    @Mock private ValidatorScriptRunner validatorRunner;
    @Mock private SqlSecurityValidator securityValidator;
    @Mock private Acknowledgment ack;

    private SubmissionKafkaConsumer consumer;

    private final H2ExecutionService h2Service = new H2ExecutionService();

    private final String initSql = """
        CREATE TABLE users (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(100)
        );
        INSERT INTO users (name) VALUES ('Alice'), ('Bob');
        """;

    private Task dqlTask;
    private Task dmlTask;

    @BeforeEach
    void setUp() {
        consumer = new SubmissionKafkaConsumer(
                submissionRepository, taskRepository,
                h2Service, minioService, validatorRunner, securityValidator
        );

        dqlTask = Task.builder()
                .taskId(1L).type(TaskType.DQL).correctSqlResponse("SELECT * FROM users")
                .initScriptKey("tasks/1/init.sql").timeLimitMs(5000).build();

        dmlTask = Task.builder()
                .taskId(2L).type(TaskType.DML).correctSqlResponse("UPDATE users SET name='Eve'")
                .initScriptKey("tasks/2/init.sql").timeLimitMs(5000).build();

        given(securityValidator.normalize(anyString())).willAnswer(inv -> inv.getArgument(0));
        given(securityValidator.validateUserSql(anyString(), any()))
                .willReturn(new SqlSecurityValidator.ValidationResult(true, null, null));

        lenient().when(securityValidator.validateAdminSql(anyString()))
                .thenReturn(new SqlSecurityValidator.ValidationResult(true, null, null));
    }

    private SqlSubmission submission(Long id, Task task, String sql) {
        return SqlSubmission.builder()
                .submissionId(id).task(task).userId(UUID.randomUUID())
                .userSqlQuery(sql).status(SubmissionStatus.PENDING).build();
    }

    private SubmissionKafkaMessage msg(Long submissionId, Long taskId, String sql) {
        return new SubmissionKafkaMessage(submissionId, taskId, UUID.randomUUID(), sql);
    }

    @Test
    void shouldAcceptCorrectDql() {
        SqlSubmission sub = submission(1L, dqlTask, "SELECT * FROM users");
        SubmissionKafkaMessage message = msg(1L, 1L, "SELECT * FROM users");

        given(submissionRepository.findById(1L)).willReturn(Optional.of(sub));
        given(taskRepository.findById(1L)).willReturn(Optional.of(dqlTask));
        given(minioService.downloadAsString("tasks/1/init.sql")).willReturn(initSql);

        consumer.consume(new ConsumerRecord<>("sql.submissions", 0, 0L, "key", message), ack);

        ArgumentCaptor<SqlSubmission> captor = ArgumentCaptor.forClass(SqlSubmission.class);
        then(submissionRepository).should().save(captor.capture());
        SqlSubmission saved = captor.getValue();

        assertThat(saved.getStatus()).isEqualTo(SubmissionStatus.DONE);
        assertThat(saved.getVerdict()).isEqualTo(SqlVerdict.ACCEPTED);
        assertThat(saved.getRowsMatched()).isTrue();
        then(ack).should().acknowledge();
    }

    @Test
    void shouldAcceptCorrectDml() {
        SqlSubmission sub = submission(2L, dmlTask, "UPDATE users SET name='Eve'");
        SubmissionKafkaMessage message = msg(2L, 2L, "UPDATE users SET name='Eve'");

        given(submissionRepository.findById(2L)).willReturn(Optional.of(sub));
        given(taskRepository.findById(2L)).willReturn(Optional.of(dmlTask));
        given(minioService.downloadAsString("tasks/2/init.sql")).willReturn(initSql);

        consumer.consume(new ConsumerRecord<>("sql.submissions", 0, 0L, "key", message), ack);

        ArgumentCaptor<SqlSubmission> captor = ArgumentCaptor.forClass(SqlSubmission.class);
        then(submissionRepository).should().save(captor.capture());
        SqlSubmission saved = captor.getValue();

        assertThat(saved.getStatus()).isEqualTo(SubmissionStatus.DONE);
        assertThat(saved.getVerdict()).isEqualTo(SqlVerdict.ACCEPTED);
        assertThat(saved.getRowsMatched()).isTrue();
    }

    @Test
    void shouldRejectDmlOnDqlTask() {
        SqlSubmission sub = submission(3L, dqlTask, "UPDATE users SET name='Eve'");
        SubmissionKafkaMessage message = msg(3L, 1L, "UPDATE users SET name='Eve'");

        given(submissionRepository.findById(3L)).willReturn(Optional.of(sub));
        given(taskRepository.findById(1L)).willReturn(Optional.of(dqlTask));

        consumer.consume(new ConsumerRecord<>("sql.submissions", 0, 0L, "key", message), ack);

        ArgumentCaptor<SqlSubmission> captor = ArgumentCaptor.forClass(SqlSubmission.class);
        then(submissionRepository).should().save(captor.capture());
        SqlSubmission saved = captor.getValue();

        assertThat(saved.getStatus()).isEqualTo(SubmissionStatus.DONE);
        assertThat(saved.getVerdict()).isEqualTo(SqlVerdict.COMPILATION_ERROR);
        assertThat(saved.getErrorMessage()).contains("DQL");
        then(ack).should().acknowledge();
    }

    @Test
    void shouldRejectSelectOnDmlTask() {
        SqlSubmission sub = submission(4L, dmlTask, "SELECT * FROM users");
        SubmissionKafkaMessage message = msg(4L, 2L, "SELECT * FROM users");

        given(submissionRepository.findById(4L)).willReturn(Optional.of(sub));
        given(taskRepository.findById(2L)).willReturn(Optional.of(dmlTask));

        consumer.consume(new ConsumerRecord<>("sql.submissions", 0, 0L, "key", message), ack);

        ArgumentCaptor<SqlSubmission> captor = ArgumentCaptor.forClass(SqlSubmission.class);
        then(submissionRepository).should().save(captor.capture());
        SqlSubmission saved = captor.getValue();

        assertThat(saved.getStatus()).isEqualTo(SubmissionStatus.DONE);
        assertThat(saved.getVerdict()).isEqualTo(SqlVerdict.COMPILATION_ERROR);
        assertThat(saved.getErrorMessage()).contains("DML");
    }

    @Test
    void shouldSetSystemErrorIfInitScriptNotFound() {
        SqlSubmission sub = submission(5L, dqlTask, "SELECT * FROM users");
        SubmissionKafkaMessage message = msg(5L, 1L, "SELECT * FROM users");

        given(submissionRepository.findById(5L)).willReturn(Optional.of(sub));
        given(taskRepository.findById(1L)).willReturn(Optional.of(dqlTask));
        given(minioService.downloadAsString("tasks/1/init.sql"))
                .willThrow(new RuntimeException("MinIO unavailable"));

        consumer.consume(new ConsumerRecord<>("sql.submissions", 0, 0L, "key", message), ack);

        ArgumentCaptor<SqlSubmission> captor = ArgumentCaptor.forClass(SqlSubmission.class);
        then(submissionRepository).should().save(captor.capture());
        SqlSubmission saved = captor.getValue();

        assertThat(saved.getStatus()).isEqualTo(SubmissionStatus.ERROR);
        assertThat(saved.getVerdict()).isEqualTo(SqlVerdict.SYSTEM_ERROR);
        assertThat(saved.getErrorMessage()).contains("Cannot load init script");
        then(ack).should().acknowledge();
    }

    @Test
    void shouldSetSystemErrorIfCorrectSqlFails() {
        Task brokenTask = Task.builder()
                .taskId(3L).type(TaskType.DQL).correctSqlResponse("INVALID SQL")
                .initScriptKey("tasks/3/init.sql").timeLimitMs(5000).build();

        SqlSubmission sub = submission(6L, brokenTask, "SELECT * FROM users");
        SubmissionKafkaMessage message = msg(6L, 3L, "SELECT * FROM users");

        given(submissionRepository.findById(6L)).willReturn(Optional.of(sub));
        given(taskRepository.findById(3L)).willReturn(Optional.of(brokenTask));
        given(minioService.downloadAsString("tasks/3/init.sql")).willReturn(initSql);

        consumer.consume(new ConsumerRecord<>("sql.submissions", 0, 0L, "key", message), ack);

        ArgumentCaptor<SqlSubmission> captor = ArgumentCaptor.forClass(SqlSubmission.class);
        then(submissionRepository).should().save(captor.capture());
        SqlSubmission saved = captor.getValue();

        assertThat(saved.getStatus()).isEqualTo(SubmissionStatus.ERROR);
        assertThat(saved.getVerdict()).isEqualTo(SqlVerdict.SYSTEM_ERROR);
        assertThat(saved.getErrorMessage()).contains("Task correct SQL error");
    }
    @Test
    void shouldSetCompilationErrorIfUserSqlHasSyntaxError() {
        SqlSubmission sub = submission(7L, dqlTask, "SELEC * FROM users");
        SubmissionKafkaMessage message = msg(7L, 1L, "SELEC * FROM users");

        given(submissionRepository.findById(7L)).willReturn(Optional.of(sub));
        given(taskRepository.findById(1L)).willReturn(Optional.of(dqlTask));
        consumer.consume(new ConsumerRecord<>("sql.submissions", 0, 0L, "key", message), ack);

        ArgumentCaptor<SqlSubmission> captor = ArgumentCaptor.forClass(SqlSubmission.class);
        then(submissionRepository).should().save(captor.capture());
        SqlSubmission saved = captor.getValue();

        assertThat(saved.getStatus()).isEqualTo(SubmissionStatus.DONE);
        assertThat(saved.getVerdict()).isEqualTo(SqlVerdict.COMPILATION_ERROR);
    }
    @Test
    void shouldSetWrongAnswerIfValidatorFails() {
        Task taskWithValidator = Task.builder()
                .taskId(4L).type(TaskType.DQL).correctSqlResponse("SELECT * FROM users")
                .initScriptKey("tasks/4/init.sql")
                .validatorScriptKey("tasks/4/Validator.java")
                .timeLimitMs(5000).build();

        SqlSubmission sub = submission(8L, taskWithValidator, "SELECT * FROM users");
        SubmissionKafkaMessage message = msg(8L, 4L, "SELECT * FROM users");

        given(submissionRepository.findById(8L)).willReturn(Optional.of(sub));
        given(taskRepository.findById(4L)).willReturn(Optional.of(taskWithValidator));
        given(minioService.downloadAsString("tasks/4/init.sql")).willReturn(initSql);
        given(minioService.downloadAsString("tasks/4/Validator.java")).willReturn("// mock code");
        given(validatorRunner.run(anyString(), anyList(), anyList()))
                .willReturn(new ValidatorScriptRunner.ValidationResult(false, "Names differ", null));

        consumer.consume(new ConsumerRecord<>("sql.submissions", 0, 0L, "key", message), ack);

        ArgumentCaptor<SqlSubmission> captor = ArgumentCaptor.forClass(SqlSubmission.class);
        then(submissionRepository).should().save(captor.capture());
        SqlSubmission saved = captor.getValue();

        assertThat(saved.getStatus()).isEqualTo(SubmissionStatus.DONE);
        assertThat(saved.getVerdict()).isEqualTo(SqlVerdict.WRONG_ANSWER);
        assertThat(saved.getErrorMessage()).contains("Names differ");
    }
}