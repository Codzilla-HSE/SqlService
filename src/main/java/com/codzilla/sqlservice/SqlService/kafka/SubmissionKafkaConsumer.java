package com.codzilla.sqlservice.SqlService.kafka;

import com.codzilla.sqlservice.SqlService.DB.SqlSubmission;
import com.codzilla.sqlservice.SqlService.DB.SubmissionRepository;
import com.codzilla.sqlservice.SqlService.DB.Task;
import com.codzilla.sqlservice.SqlService.DB.TaskRepository;
import com.codzilla.sqlservice.SqlService.Dto.SqlExecutionResult;
import com.codzilla.sqlservice.SqlService.Service.H2ExecutionService;
import com.codzilla.sqlservice.SqlService.Service.MinioService;
import com.codzilla.sqlservice.SqlService.Service.SqlSecurityValidator;
import com.codzilla.sqlservice.SqlService.Service.ValidatorScriptRunner;
import com.codzilla.sqlservice.SqlService.model.SqlVerdict;
import com.codzilla.sqlservice.SqlService.model.SubmissionStatus;
import com.codzilla.sqlservice.SqlService.model.TaskType;
import com.codzilla.sqlservice.SqlService.Dto.SubmissionKafkaMessage;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class SubmissionKafkaConsumer {

    private final SubmissionRepository submissionRepository;
    private final TaskRepository taskRepository;
    private final H2ExecutionService h2ExecutionService;
    private final MinioService minioService;
    private final ValidatorScriptRunner validatorRunner;
    private final SqlSecurityValidator securityValidator;

    @KafkaListener(
            topics = KafkaConfig.SUBMISSION_TOPIC,
            groupId = "sql-judge-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void consume(ConsumerRecord<String, SubmissionKafkaMessage> record,
                        Acknowledgment ack) {

        SubmissionKafkaMessage message = record.value();
        long kafkaOffset = record.offset();

        log.info("Processing submission {} offset={}", message.submissionId(), kafkaOffset);

        SqlSubmission submission = submissionRepository.findById(message.submissionId())
                .orElse(null);

        if (submission == null) {
            log.error("Submission {} not found, skipping", message.submissionId());
            ack.acknowledge();
            return;
        }

        submission.setKafkaOffset(kafkaOffset);

        try {
            processSubmission(submission, message, ack);
        } catch (Exception e) {
            log.error("Unexpected error for submission {}", message.submissionId(), e);
            finalize(submission, SubmissionStatus.ERROR, SqlVerdict.SYSTEM_ERROR,
                    false, 0L, "Unexpected system error: " + e.getMessage());
            submissionRepository.save(submission);
            ack.acknowledge();
        }
    }

    private void processSubmission(SqlSubmission submission,
                                   SubmissionKafkaMessage message,
                                   Acknowledgment ack) {
        Task task = taskRepository.findById(message.taskId()).orElse(null);

        if (task == null) {
            finalize(submission, SubmissionStatus.ERROR, SqlVerdict.SYSTEM_ERROR,
                    false, 0L, "Task not found: " + message.taskId());
            submissionRepository.save(submission);
            ack.acknowledge();
            return;
        }

        int timeLimit = task.getTimeLimitMs() != null ? task.getTimeLimitMs() : 30_000;

        // ── Нормализация и проверка безопасности ──────────────────────────
        String normalizedUserSql = securityValidator.normalize(message.userSqlQuery());

        SqlSecurityValidator.ValidationResult secCheck =
                securityValidator.validateUserSql(normalizedUserSql, task.getType());

        if (!secCheck.valid()) {
            finalize(submission, SubmissionStatus.DONE, secCheck.failVerdict(),
                    false, 0L, secCheck.message());
            submissionRepository.save(submission);
            ack.acknowledge();
            return;
        }

        // ── Проверка соответствия типа SQL типу задачи ─────────────────────
        String upperSql = normalizedUserSql.trim().toUpperCase();
        boolean isSelect = upperSql.startsWith("SELECT");
        boolean isDml = upperSql.startsWith("INSERT") || upperSql.startsWith("UPDATE") ||
                upperSql.startsWith("DELETE") || upperSql.startsWith("MERGE");

        if (task.getType() == TaskType.DQL && !isSelect) {
            finalize(submission, SubmissionStatus.DONE, SqlVerdict.COMPILATION_ERROR,
                    false, 0L, "Задача типа DQL принимает только SELECT-запросы");
            submissionRepository.save(submission);
            ack.acknowledge();
            return;
        }
        if (task.getType() == TaskType.DML && !isDml) {
            finalize(submission, SubmissionStatus.DONE, SqlVerdict.COMPILATION_ERROR,
                    false, 0L, "Задача типа DML принимает только INSERT/UPDATE/DELETE/MERGE");
            submissionRepository.save(submission);
            ack.acknowledge();
            return;
        }

        // ── Проверка correct_sql от составителя ────────────────────────────
        String normalizedCorrectSql = securityValidator.normalize(task.getCorrectSqlResponse());
        SqlSecurityValidator.ValidationResult adminCheck =
                securityValidator.validateAdminSql(normalizedCorrectSql);

        if (!adminCheck.valid()) {
            finalize(submission, SubmissionStatus.ERROR, SqlVerdict.SYSTEM_ERROR,
                    false, 0L, "Task configuration error: " + adminCheck.message());
            submissionRepository.save(submission);
            ack.acknowledge();
            return;
        }

        // ── Загрузка init.sql ──────────────────────────────────────────────
        String initSql;
        try {
            initSql = loadInitScript(task);
        } catch (Exception e) {
            finalize(submission, SubmissionStatus.ERROR, SqlVerdict.SYSTEM_ERROR,
                    false, 0L, "Cannot load init script: " + e.getMessage());
            submissionRepository.save(submission);
            ack.acknowledge();
            return;
        }

        // ── Выполнение correct_sql (эталон) ────────────────────────────────
        SqlExecutionResult correctResult = h2ExecutionService.executeInIsolation(
                -(message.submissionId()), initSql, normalizedCorrectSql, timeLimit, task.getType());

        if (!correctResult.success()) {
            finalize(submission, SubmissionStatus.ERROR, SqlVerdict.SYSTEM_ERROR,
                    false, correctResult.executionTimeMs(),
                    "Task correct SQL error: " + correctResult.errorMessage());
            submissionRepository.save(submission);
            ack.acknowledge();
            return;
        }

        // ── Выполнение user_sql ────────────────────────────────────────────
        SqlExecutionResult userResult = h2ExecutionService.executeInIsolation(
                message.submissionId(), initSql, normalizedUserSql, timeLimit, task.getType());

        if (!userResult.success()) {
            finalize(submission, SubmissionStatus.DONE, userResult.failVerdict(),
                    false, userResult.executionTimeMs(), userResult.errorMessage());
            submissionRepository.save(submission);
            ack.acknowledge();
            return;
        }

        // ── Валидация результата ───────────────────────────────────────────
        ValidatorScriptRunner.ValidationResult validation;
        try {
            validation = runValidation(task, correctResult.rows(), userResult.rows());
        } catch (Exception e) {
            finalize(submission, SubmissionStatus.ERROR, SqlVerdict.SYSTEM_ERROR,
                    false, userResult.executionTimeMs(), "Validator error: " + e.getMessage());
            submissionRepository.save(submission);
            ack.acknowledge();
            return;
        }

        boolean accepted = !validation.hasError() && validation.accepted();
        SqlVerdict verdict = accepted ? SqlVerdict.ACCEPTED : SqlVerdict.WRONG_ANSWER;
        String failMsg = validation.hasError() ? validation.errorMessage() : validation.failMessage();

        finalize(submission, SubmissionStatus.DONE, verdict,
                accepted, userResult.executionTimeMs(), failMsg);
        submissionRepository.save(submission);

        log.info("Submission {} → {} in {}ms",
                message.submissionId(), verdict, userResult.executionTimeMs());

        ack.acknowledge();
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private void finalize(SqlSubmission s, SubmissionStatus status, SqlVerdict verdict,
                          boolean rowsMatched, Long execTime, String errorMessage) {
        s.setStatus(status);
        s.setVerdict(verdict);
        s.setRowsMatched(rowsMatched);
        s.setExecutionTimeMs(execTime);
        if (errorMessage != null) s.setErrorMessage(errorMessage);
    }

    private String loadInitScript(Task task) {
        if (task.getInitScriptKey() == null || task.getInitScriptKey().isBlank()) {
            return "";
        }
        return minioService.downloadAsString(task.getInitScriptKey());
    }

    private ValidatorScriptRunner.ValidationResult runValidation(
            Task task,
            List<Map<String, Object>> correctRows,
            List<Map<String, Object>> userRows) {

        if (task.getValidatorScriptKey() != null && !task.getValidatorScriptKey().isBlank()) {
            String code = minioService.downloadAsString(task.getValidatorScriptKey());
            return validatorRunner.run(code, correctRows, userRows);
        }

        // Дефолтное сравнение
        List<String> c = correctRows.stream().map(Map::toString).sorted().toList();
        List<String> u = userRows.stream().map(Map::toString).sorted().toList();
        boolean match = c.equals(u);
        return new ValidatorScriptRunner.ValidationResult(match,
                match ? null : "Ожидалось " + correctRows.size() +
                        " строк, получено " + userRows.size(), null);
    }
}