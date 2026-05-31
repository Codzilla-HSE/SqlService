package com.codzilla.sqlservice.SqlService.Service;

import com.codzilla.sqlservice.SqlService.model.SqlVerdict;
import com.codzilla.sqlservice.SqlService.model.TaskType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class SqlSecurityValidatorTest {

    private SqlSecurityValidator validator;

    @BeforeEach
    void setUp() {
        validator = new SqlSecurityValidator();
    }



    @Test
    void normalize_removesSingleLineComments() {
        String sql = "SELECT * -- comment\nFROM users";
        assertThat(validator.normalize(sql)).isEqualTo("SELECT * FROM users");
    }

    @Test
    void normalize_removesMultiLineComments() {
        String sql = "SELECT /* inline */ * FROM users";
        assertThat(validator.normalize(sql)).isEqualTo("SELECT * FROM users");
    }

    @Test
    void normalize_collapsesWhitespace() {
        String sql = "SELECT   * \t\n FROM   users";
        assertThat(validator.normalize(sql)).isEqualTo("SELECT * FROM users");
    }

    @Test
    void normalize_removesTrailingSemicolon() {
        String sql = "SELECT * FROM users;";
        assertThat(validator.normalize(sql)).isEqualTo("SELECT * FROM users");
    }

    @Test
    void normalize_handlesMultipleTrailingSemicolons() {
        String sql = "SELECT * FROM users;;;";
        assertThat(validator.normalize(sql)).isEqualTo("SELECT * FROM users");
    }

    @Test
    void validateUserSql_nullOrEmpty_returnsCompilationError() {
        assertThat(validator.validateUserSql(null, TaskType.DQL).valid()).isFalse();
        assertThat(validator.validateUserSql("   ", TaskType.DML).valid()).isFalse();
    }

    @Test
    void validateUserSql_tooLong_returnsSecurityViolation() {
        String longSql = "A".repeat(4001);
        var result = validator.validateUserSql(longSql, TaskType.DQL);
        assertThat(result.valid()).isFalse();
        assertThat(result.failVerdict()).isEqualTo(SqlVerdict.SECURITY_VIOLATION);
    }
    @ParameterizedTest
    @ValueSource(strings = {
            "DROP TABLE users",
            "TRUNCATE TABLE users",
            "CREATE TABLE t(id INT)",
            "CREATE TABLE x(id INT)",
            "ALTER TABLE users ADD COLUMN x INT",
            "COMMIT",
            "ROLLBACK",
            "SAVEPOINT sp1",
            "BEGIN TRANSACTION",
            "CALL CSVWRITE('file.csv', 'SELECT *')",
            "FILE_READ('pass.txt')",
            "LINKED TABLE mylink",
            "INFORMATION_SCHEMA.TABLES",
            "PG_CATALOG.pg_class",
            "SELECT * FROM users; DROP TABLE users",
            "SLEEP(10)",
            "PG_SLEEP(10)",
            "BENCHMARK(1000000,MD5('x'))",
            "WAITFOR DELAY '00:00:10'",
            "SYSTEM command",
            "SHELL command"
    })

    void validateUserSql_forbiddenPatterns_returnsSecurityViolation(String maliciousSql) {
        var result = validator.validateUserSql(maliciousSql, TaskType.DQL);
        assertThat(result.valid()).isFalse();
        assertThat(result.failVerdict()).isEqualTo(SqlVerdict.SECURITY_VIOLATION);
    }

    @Test
    void validateUserSql_dqlWithSelect_isValid() {
        var result = validator.validateUserSql("SELECT * FROM users", TaskType.DQL);
        assertThat(result.valid()).isTrue();
    }

    @Test
    void validateUserSql_dqlWithWithClause_isValid() {
        var result = validator.validateUserSql("WITH cte AS (SELECT 1) SELECT * FROM cte", TaskType.DQL);
        assertThat(result.valid()).isTrue();
    }

    @Test
    void validateUserSql_dqlWithInsert_returnsCompilationError() {
        var result = validator.validateUserSql("INSERT INTO users VALUES (1)", TaskType.DQL);
        assertThat(result.valid()).isFalse();
        assertThat(result.failVerdict()).isEqualTo(SqlVerdict.COMPILATION_ERROR);
        assertThat(result.message()).contains("SELECT");
    }

    @Test
    void validateUserSql_dmlWithUpdate_isValid() {
        var result = validator.validateUserSql("UPDATE users SET name='A'", TaskType.DML);
        assertThat(result.valid()).isTrue();
    }

    @Test
    void validateUserSql_dmlWithInsert_isValid() {
        var result = validator.validateUserSql("INSERT INTO users VALUES (1)", TaskType.DML);
        assertThat(result.valid()).isTrue();
    }

    @Test
    void validateUserSql_dmlWithDelete_isValid() {
        var result = validator.validateUserSql("DELETE FROM users", TaskType.DML);
        assertThat(result.valid()).isTrue();
    }

    @Test
    void validateUserSql_dmlWithSelect_returnsCompilationError() {
        var result = validator.validateUserSql("SELECT * FROM users", TaskType.DML);
        assertThat(result.valid()).isFalse();
        assertThat(result.failVerdict()).isEqualTo(SqlVerdict.COMPILATION_ERROR);
        assertThat(result.message()).contains("INSERT/UPDATE/DELETE/MERGE");
    }

    @Test
    void validateUserSql_ddl_returnsSecurityViolation() {
        var result = validator.validateUserSql("CREATE TABLE t(id INT)", TaskType.DDL);
        assertThat(result.valid()).isFalse();
        assertThat(result.failVerdict()).isEqualTo(SqlVerdict.SECURITY_VIOLATION);
    }



    @Test
    void validateAdminSql_nullOrEmpty_returnsCompilationError() {
        assertThat(validator.validateAdminSql(null).valid()).isFalse();
        assertThat(validator.validateAdminSql("   ").valid()).isFalse();
    }

    @Test
    void validateAdminSql_tooLong_returnsCompilationError() {
        String longSql = "A".repeat(8001);
        var result = validator.validateAdminSql(longSql);
        assertThat(result.valid()).isFalse();
        assertThat(result.failVerdict()).isEqualTo(SqlVerdict.COMPILATION_ERROR);
    }

    @Test
    void validateAdminSql_allowsDmlAndDdl() {

        assertThat(validator.validateAdminSql("UPDATE users SET name='A'").valid()).isTrue();
        assertThat(validator.validateAdminSql("CREATE TABLE test(id INT)").valid()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CALL CSVWRITE('f','S')",
            "FILE_READ('f')",
            "SYSTEM cmd",
            "SHELL cmd",
            "SLEEP(10)",
            "BENCHMARK(1000,MD5('x'))",
            "WAITFOR DELAY '00:00:10'"
    })
    void validateAdminSql_forbiddenSystemCalls_returnsSecurityViolation(String adminSql) {
        var result = validator.validateAdminSql(adminSql);
        assertThat(result.valid()).isFalse();
        assertThat(result.failVerdict()).isEqualTo(SqlVerdict.SECURITY_VIOLATION);
    }
}