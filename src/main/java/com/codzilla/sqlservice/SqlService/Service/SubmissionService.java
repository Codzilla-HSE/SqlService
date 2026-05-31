package com.codzilla.sqlservice.SqlService.Service;

import com.codzilla.sqlservice.SqlService.DB.*;
import com.codzilla.sqlservice.SqlService.Dto.SubmissionStatusDto;
import com.codzilla.sqlservice.SqlService.kafka.KafkaConfig;
import com.codzilla.sqlservice.SqlService.Dto.SubmissionKafkaMessage;
import com.codzilla.sqlservice.SqlService.model.SqlVerdict;
import com.codzilla.sqlservice.SqlService.model.SubmissionStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class SubmissionService {

    private final SubmissionRepository submissionRepository;
    private final TaskRepository taskRepository;
    private final KafkaTemplate<String, SubmissionKafkaMessage> kafkaTemplate;
    private final SqlSecurityValidator securityValidator; // ← добавлено

    @Transactional
    public Long submit(Long taskId, UUID userId, String userSqlQuery) {
        Task task = taskRepository.findById(taskId)
                .orElseThrow(() -> new IllegalArgumentException("Task not found: " + taskId));

        // ── Ранняя валидация до сохранения в БД ─────────────────────────────
        // Нормализуем сразу — сохраняем нормализованный запрос
        String normalizedSql = securityValidator.normalize(userSqlQuery);

        SqlSecurityValidator.ValidationResult check =
                securityValidator.validateUserSql(normalizedSql, task.getType());

        if (!check.valid()) {
            // Бросаем исключение — GlobalExceptionHandler вернёт 400
            // Посылка вообще не создаётся в БД
            throw new IllegalArgumentException(
                    check.failVerdict() + ": " + check.message());
        }

        // ── Создаём посылку ──────────────────────────────────────────────────
        SqlSubmission submission = SqlSubmission.builder()
                .task(task)
                .userId(userId)
                .userSqlQuery(normalizedSql)   // сохраняем нормализованный
                .status(SubmissionStatus.PENDING)
                .verdict(SqlVerdict.SYSTEM_ERROR) // placeholder
                .build();

        SqlSubmission saved = submissionRepository.save(submission);
        Long submissionId = saved.getSubmissionId();
        log.info("Submission {} saved for task {} by user {}", submissionId, taskId, userId);

        // ── Отправляем в Kafka ───────────────────────────────────────────────
        SubmissionKafkaMessage message = new SubmissionKafkaMessage(
                submissionId, taskId, userId, normalizedSql);

        CompletableFuture<SendResult<String, SubmissionKafkaMessage>> future =
                kafkaTemplate.send(KafkaConfig.SUBMISSION_TOPIC, taskId.toString(), message);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send submission {} to Kafka", submissionId, ex);
                // Kafka упала — немедленно ставим ERROR чтобы не зависнуть в PENDING
                markKafkaFailure(submissionId, ex.getMessage());
            } else {
                log.debug("Submission {} → partition={} offset={}",
                        submissionId,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return submissionId;
    }

    @Transactional(readOnly = true)
    public SubmissionStatusDto getStatus(Long submissionId) {
        SqlSubmission s = submissionRepository.findById(submissionId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Submission not found: " + submissionId));
        return new SubmissionStatusDto(
                s.getSubmissionId(), s.getStatus(), s.getVerdict(),
                s.getExecutionTimeMs(), s.getErrorMessage(), s.getKafkaOffset());
    }

    // Вызывается из async callback — отдельная транзакция
    private void markKafkaFailure(Long submissionId, String error) {
        submissionRepository.findById(submissionId).ifPresent(s -> {
            s.setStatus(SubmissionStatus.ERROR);
            s.setVerdict(SqlVerdict.SYSTEM_ERROR);
            s.setErrorMessage("Kafka unavailable: " + error);
            submissionRepository.save(s);
        });
    }
}