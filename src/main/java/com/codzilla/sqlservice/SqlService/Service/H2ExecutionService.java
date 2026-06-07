package com.codzilla.sqlservice.SqlService.Service;

import com.codzilla.sqlservice.SqlService.Dto.SqlExecutionResult;
import com.codzilla.sqlservice.SqlService.model.TaskType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@Service
public class H2ExecutionService {

    public SqlExecutionResult executeInIsolation(Long submissionId,
                                                 String initSql,
                                                 String userSql,
                                                 int timeLimitMs,
                                                 TaskType taskType) {
        String dbName = "h2_submission_" + submissionId;
        EmbeddedDatabase db = null;

        try {
            db = new EmbeddedDatabaseBuilder()
                    .setType(EmbeddedDatabaseType.H2)
                    .setName(dbName + ";DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false")
                    .build();

            JdbcTemplate jdbc = new JdbcTemplate(db);

            executeScript(jdbc, initSql);

            List<String> tableNames = extractTableNames(initSql);

            long startTime = System.currentTimeMillis();

            List<Map<String, Object>> rows;

            if (taskType == TaskType.DML) {
                jdbc.update(userSql);
                rows = selectAllFromTables(jdbc, tableNames);
            } else {
                rows = jdbc.queryForList(userSql);
            }

            long elapsed = System.currentTimeMillis() - startTime;

            if (elapsed > timeLimitMs) {
                return SqlExecutionResult.timeLimitExceeded(elapsed);
            }

            log.debug("Submission {} executed in {}ms, {} rows", submissionId, elapsed, rows.size());
            return SqlExecutionResult.success(rows, elapsed);
        } catch (Exception e) {
            log.warn("SQL execution error for submission {}: {}", submissionId, e.getMessage());
            if (e.getMessage() != null && e.getMessage().contains("bad SQL grammar")) {
                return SqlExecutionResult.compilationError(e.getMessage(), 0);
            }
            return SqlExecutionResult.runtimeError(e.getMessage(), 0);
        } finally {
            if (db != null) {
                db.shutdown();
            }
        }
    }

    /** Выполняет init-скрипт, разбивая его по ';' */
    private void executeScript(JdbcTemplate jdbc, String sql) {
        String[] lines = sql.split("\\r?\\n");
        StringBuilder currentStatement = new StringBuilder();
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("--")) {
                continue;
            }
            if (trimmed.endsWith(";")) {
                currentStatement.append(trimmed, 0, trimmed.length() - 1);
                String stmt = currentStatement.toString().trim();
                if (!stmt.isEmpty()) {
                    try {
                        jdbc.execute(stmt);
                    } catch (Exception e) {
                        throw new IllegalStateException(
                                "Init script failed at statement: [" + stmt + "]: " + e.getMessage(), e);
                    }
                }
                currentStatement.setLength(0);
            } else {
                currentStatement.append(trimmed).append(" ");
            }
        }
        String remaining = currentStatement.toString().trim();
        if (!remaining.isEmpty()) {
            try {
                jdbc.execute(remaining);
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Init script failed at statement: [" + remaining + "]: " + e.getMessage(), e);
            }
        }
    }
    /** Извлекает имена таблиц из CREATE TABLE в init.sql */
    private List<String> extractTableNames(String initSql) {
        Pattern pattern = Pattern.compile("CREATE\\s+TABLE\\s+IF\\s+NOT\\s+EXISTS\\s+(\\w+)",
                Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(initSql);
        List<String> tables = new ArrayList<>();
        while (matcher.find()) {
            tables.add(matcher.group(1));
        }
        if (tables.isEmpty()) {
            Pattern simple = Pattern.compile("CREATE\\s+TABLE\\s+(\\w+)", Pattern.CASE_INSENSITIVE);
            Matcher m = simple.matcher(initSql);
            while (m.find()) {
                tables.add(m.group(1));
            }
        }
        return tables;
    }

    /** Выполняет SELECT * FROM для всех таблиц и объединяет результаты */
    private List<Map<String, Object>> selectAllFromTables(JdbcTemplate jdbc, List<String> tableNames) {
        List<Map<String, Object>> allRows = new ArrayList<>();
        for (String table : tableNames) {
            try {
                List<Map<String, Object>> rows = jdbc.queryForList("SELECT * FROM " + table);
                for (Map<String, Object> row : rows) {
                    Map<String, Object> enriched = new LinkedHashMap<>(row);
                    enriched.put("_table", table);
                    allRows.add(enriched);
                }
            } catch (Exception e) {
                log.warn("Cannot SELECT from table {}: {}", table, e.getMessage());
            }
        }
        return allRows;
    }
}