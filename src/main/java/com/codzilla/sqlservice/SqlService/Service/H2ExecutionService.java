package com.codzilla.sqlservice.SqlService.Service;

import com.codzilla.sqlservice.SqlService.Dto.SqlExecutionResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.datasource.init.ScriptUtils;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;

import java.util.List;
import java.util.Map;

/**
 * Выполняет SQL над изолированной H2 in-memory БД.
 *
 * Каждый вызов executeInIsolation():
 *   1. Создаёт НОВУЮ H2 базу (уникальное имя = submissionId)
 *   2. Запускает init-скрипт (CREATE TABLE + INSERT)
 *   3. Выполняет переданный SQL-запрос
 *   4. Закрывает и уничтожает H2 базу
 *
 * Изоляция полная — пользователи не влияют друг на друга.
 * H2 работает в памяти — старт ~10-50мс vs 3-5 сек для Docker-контейнера.
 */
@Slf4j
@Service
public class H2ExecutionService {

    /**
     * Выполнить SQL в изолированной H2 БД с init-скриптом.
     *
     * @param submissionId уникальный идентификатор — имя БД (гарантирует изоляцию)
     * @param initSql      DDL + DML для создания схемы и данных
     * @param userSql      запрос пользователя
     * @param timeLimitMs  лимит времени
     */
    public SqlExecutionResult executeInIsolation(Long submissionId,
                                                 String initSql,
                                                 String userSql,
                                                 int timeLimitMs) {

        String dbName = "h2_submission_" + submissionId;
        EmbeddedDatabase db = null;

        try {

            db = new EmbeddedDatabaseBuilder()
                    .setType(EmbeddedDatabaseType.H2)
                    .setName(dbName + ";DB_CLOSE_DELAY=-1")
                    .build();

            JdbcTemplate jdbc = new JdbcTemplate(db);
            executeScript(jdbc, initSql);
            long startTime = System.currentTimeMillis();

            List<Map<String, Object>> rows;
            try {
                rows = jdbc.queryForList(userSql);
            } catch (org.springframework.dao.DataAccessException e) {
                long elapsed = System.currentTimeMillis() - startTime;
                log.warn("SQL execution error for submission {}: {}", submissionId, e.getMessage());

                if (e instanceof org.springframework.jdbc.BadSqlGrammarException) {
                    return SqlExecutionResult.compilationError(e.getMessage(), elapsed);
                }
                return SqlExecutionResult.runtimeError(e.getMessage(), elapsed);
            }

            long elapsed = System.currentTimeMillis() - startTime;

            if (elapsed > timeLimitMs) {
                return SqlExecutionResult.timeLimitExceeded(elapsed);
            }

            log.debug("Submission {} executed in {}ms, {} rows", submissionId, elapsed, rows.size());
            return SqlExecutionResult.success(rows, elapsed);

        } catch (Exception e) {
            log.error("H2 setup error for submission {}: {}", submissionId, e.getMessage());
            return SqlExecutionResult.runtimeError("DB init failed: " + e.getMessage(), 0);
        } finally {

            if (db != null) {
                db.shutdown();
                log.debug("H2 db {} destroyed", dbName);
            }
        }
    }



    private void executeScript(JdbcTemplate jdbc, String sql) {
        try (Connection conn = jdbc.getDataSource().getConnection()) {
            ScriptUtils.executeSqlScript(conn,
                    new ByteArrayResource(sql.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException("Init script failed: " + e.getMessage(), e);
        }
    }
}