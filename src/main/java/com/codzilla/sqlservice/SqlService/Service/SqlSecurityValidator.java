package com.codzilla.sqlservice.SqlService.Service;

import com.codzilla.sqlservice.SqlService.model.SqlVerdict;
import com.codzilla.sqlservice.SqlService.model.TaskType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.regex.Pattern;


@Slf4j
@Component
public class SqlSecurityValidator {

    private static final int MAX_SQL_LENGTH = 4_000;

    private static final List<Pattern> FORBIDDEN_PATTERNS = List.of(
            Pattern.compile("(?i)\\b(DROP|TRUNCATE|CREATE|ALTER|RENAME)\\b"),
            Pattern.compile("(?i)\\b(COMMIT|ROLLBACK|SAVEPOINT|BEGIN\\s+TRANSACTION)\\b"),
            Pattern.compile("(?i)\\bCALL\\s+CSVWRITE\\b"),
            Pattern.compile("(?i)\\bFILE_READ\\b"),
            Pattern.compile("(?i)\\bLINKED\\s+TABLE\\b"),
            Pattern.compile("(?i)\\bINFORMATION_SCHEMA\\b"),
            Pattern.compile("(?i)\\bPG_CATALOG\\b"),
            Pattern.compile("(?i);\\s*(DROP|DELETE|UPDATE|INSERT|CREATE|ALTER)"),
            Pattern.compile("(?i)\\b(SLEEP|PG_SLEEP|BENCHMARK|WAITFOR\\s+DELAY)\\b"),
            Pattern.compile("(?i)\\bEXEC(UTE)?\\s*\\("),
            Pattern.compile("(?i)\\bSYSTEM\\b"),
            Pattern.compile("(?i)\\bSHELL\\b")
    );


    public record ValidationResult(boolean valid, SqlVerdict failVerdict, String message) {
        public static ValidationResult ok() {
            return new ValidationResult(true, null, null);
        }
        public static ValidationResult fail(SqlVerdict verdict, String message) {
            return new ValidationResult(false, verdict, message);
        }
    }


    public ValidationResult validateUserSql(String sql, TaskType taskType) {
        if (sql == null || sql.isBlank()) {
            return ValidationResult.fail(SqlVerdict.COMPILATION_ERROR, "SQL запрос не может быть пустым");
        }
        String trimmed = sql.strip();
        if (trimmed.length() > MAX_SQL_LENGTH) {
            return ValidationResult.fail(SqlVerdict.SECURITY_VIOLATION,
                    "SQL запрос слишком длинный (макс. " + MAX_SQL_LENGTH + " символов)");
        }


        for (Pattern pattern : FORBIDDEN_PATTERNS) {
            if (pattern.matcher(trimmed).find()) {
                log.warn("Blocked forbidden SQL pattern [{}] in query: {}",
                        pattern.pattern(), trimmed.substring(0, Math.min(100, trimmed.length())));
                return ValidationResult.fail(SqlVerdict.SECURITY_VIOLATION,
                        "Запрос содержит запрещённую конструкцию");
            }
        }

        ValidationResult typeCheck = validateByTaskType(trimmed, taskType);
        if (!typeCheck.valid()) return typeCheck;

        return ValidationResult.ok();
    }


    private ValidationResult validateByTaskType(String sql, TaskType taskType) {
        String upper = sql.toUpperCase().stripLeading();

        return switch (taskType) {
            case DQL -> {
                if (!upper.startsWith("SELECT") && !upper.startsWith("WITH")) {
                    yield ValidationResult.fail(SqlVerdict.COMPILATION_ERROR,
                            "Задача типа DQL принимает только SELECT-запросы");
                }
                yield ValidationResult.ok();
            }
            case DML -> {
                boolean isDml = upper.startsWith("INSERT") ||
                        upper.startsWith("UPDATE") ||
                        upper.startsWith("DELETE") ||
                        upper.startsWith("MERGE");
                if (!isDml) {
                    yield ValidationResult.fail(SqlVerdict.COMPILATION_ERROR,
                            "Задача типа DML принимает только INSERT/UPDATE/DELETE/MERGE");
                }
                yield ValidationResult.ok();
            }
            case DDL -> {
                yield ValidationResult.fail(SqlVerdict.SECURITY_VIOLATION,
                        "DDL задачи недоступны для пользовательских посылок");
            }
            case DCL, TCL -> {
                yield ValidationResult.fail(SqlVerdict.SECURITY_VIOLATION,
                        "Задачи типа " + taskType + " недоступны для посылок");
            }
        };
    }


    public ValidationResult validateAdminSql(String sql) {
        if (sql == null || sql.isBlank()) {
            return ValidationResult.fail(SqlVerdict.COMPILATION_ERROR,
                    "Правильный ответ не может быть пустым");
        }
        if (sql.length() > MAX_SQL_LENGTH * 2) {
            return ValidationResult.fail(SqlVerdict.COMPILATION_ERROR,
                    "Слишком длинный SQL в правильном ответе");
        }

        List<Pattern> adminForbidden = List.of(
                Pattern.compile("(?i)\\bCALL\\s+CSVWRITE\\b"),
                Pattern.compile("(?i)\\bFILE_READ\\b"),
                Pattern.compile("(?i)\\bSYSTEM\\b"),
                Pattern.compile("(?i)\\bSHELL\\b"),
                Pattern.compile("(?i)\\b(SLEEP|BENCHMARK|WAITFOR\\s+DELAY)\\b")
        );

        for (Pattern p : adminForbidden) {
            if (p.matcher(sql).find()) {
                return ValidationResult.fail(SqlVerdict.SECURITY_VIOLATION,
                        "correct_sql содержит запрещённую системную конструкцию");
            }
        }
        return ValidationResult.ok();
    }


    public String normalize(String sql) {
        return sql
                .replaceAll("--[^\n]*", "")
                .replaceAll("/\\*.*?\\*/", " ")
                .replaceAll("\\s+", " ")
                .stripTrailing()
                .replaceAll(";+$", "")
                .strip();
    }
}