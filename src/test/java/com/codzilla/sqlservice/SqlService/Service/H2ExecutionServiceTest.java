package com.codzilla.sqlservice.SqlService.Service;

import com.codzilla.sqlservice.SqlService.Dto.SqlExecutionResult;
import com.codzilla.sqlservice.SqlService.model.SqlVerdict;
import com.codzilla.sqlservice.SqlService.model.TaskType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class H2ExecutionServiceTest {

    private H2ExecutionService service;

    private final String initSqlUsers = """
        CREATE TABLE users (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(100) NOT NULL
        );
        INSERT INTO users (name) VALUES ('Alice'), ('Bob');
    """;

    private final String initSqlEmployees = """
        CREATE TABLE employees (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(100) NOT NULL,
            salary DECIMAL(10,2) NOT NULL
        );
        INSERT INTO employees (name, salary) VALUES ('Ivan', 50000), ('Maria', 60000);
    """;

    @BeforeEach
    void setUp() {
        service = new H2ExecutionService();
    }

    @Test
    void dql_selectAll_returnsAllRows() {
        SqlExecutionResult result = service.executeInIsolation(
                1L, initSqlUsers, "SELECT * FROM users", 5000, TaskType.DQL);

        assertTrue(result.success());
        assertEquals(2, result.rows().size());

        List<String> names = result.rows().stream()
                .map(r -> (String) r.get("name"))   // строчные буквы
                .toList();
        assertTrue(names.contains("Alice"));
        assertTrue(names.contains("Bob"));

        // Убедимся, что поле _table не добавляется для DQL
        assertFalse(result.rows().get(0).containsKey("_table"));
    }

    @Test
    void dql_withConditionAndOrder_returnsCorrectRows() {
        SqlExecutionResult result = service.executeInIsolation(
                2L, initSqlEmployees, "SELECT * FROM employees WHERE id > 1 ORDER BY name DESC", 5000, TaskType.DQL);

        assertTrue(result.success());
        assertEquals(1, result.rows().size());
        assertEquals("Maria", result.rows().get(0).get("name"));   // строчные
    }

    @Test
    void dml_update_modifiesDataAndReturnsEnrichedRows() {
        String updateSql = "UPDATE employees SET name = 'Eve'";
        SqlExecutionResult result = service.executeInIsolation(
                3L, initSqlEmployees, updateSql, 5000, TaskType.DML);

        assertTrue(result.success());
        assertEquals(2, result.rows().size());

        // Все строки должны иметь name = 'Eve'
        assertTrue(result.rows().stream().allMatch(r -> "Eve".equals(r.get("name"))));

        // Должно присутствовать поле _table
        assertTrue(result.rows().get(0).containsKey("_table"));
        assertEquals("employees", result.rows().get(0).get("_table"));
    }

    @Test
    void dml_insert_addsRowAndReturnsAllRows() {
        String insertSql = "INSERT INTO employees (name, salary) VALUES ('Alex', 55000)";
        SqlExecutionResult result = service.executeInIsolation(
                4L, initSqlEmployees, insertSql, 5000, TaskType.DML);

        assertTrue(result.success());
        assertEquals(3, result.rows().size());
        List<String> names = result.rows().stream()
                .map(r -> (String) r.get("name"))   // строчные
                .toList();
        assertTrue(names.contains("Alex"));
    }

    @Test
    void dml_delete_removesRowAndReturnsRemainingRows() {
        String deleteSql = "DELETE FROM employees WHERE id = 2";
        SqlExecutionResult result = service.executeInIsolation(
                5L, initSqlEmployees, deleteSql, 5000, TaskType.DML);

        assertTrue(result.success());
        assertEquals(1, result.rows().size());
        assertEquals("Ivan", result.rows().get(0).get("name"));   // строчные
    }

    @Test
    void badSqlGrammar_returnsCompilationError() {
        SqlExecutionResult result = service.executeInIsolation(
                6L, initSqlUsers, "SELEC * FROM users", 5000, TaskType.DQL);

        assertFalse(result.success());
        assertEquals(SqlVerdict.COMPILATION_ERROR, result.failVerdict());
        assertNotNull(result.errorMessage());
        assertTrue(result.errorMessage().contains("bad SQL grammar"));
    }

    @Test
    void runtimeError_exceptionDuringExecution_returnsRuntimeError() {
        String badQuery = "SELECT * FROM users WHERE 1/0 = 1";
        SqlExecutionResult result = service.executeInIsolation(
                7L, initSqlUsers, badQuery, 5000, TaskType.DQL);

        assertFalse(result.success());
        assertEquals(SqlVerdict.RUNTIME_ERROR, result.failVerdict());
    }

    @Test
    void initScriptContainsComments_skipsThemAndSucceeds() {
        String initWithComments = """
            -- This is a comment
            CREATE TABLE test (id INT);
            -- Another comment
            INSERT INTO test (id) VALUES (1);
        """;
        SqlExecutionResult result = service.executeInIsolation(
                8L, initWithComments, "SELECT * FROM test", 5000, TaskType.DQL);

        assertTrue(result.success());
        assertEquals(1, result.rows().size());
    }

    @Test
    void multipleTables_returnsRowsFromAllTablesInDml() {
        String multiTableInit = """
            CREATE TABLE a (id INT);
            CREATE TABLE b (id INT);
            INSERT INTO a VALUES (1);
            INSERT INTO b VALUES (2);
        """;
        SqlExecutionResult result = service.executeInIsolation(
                9L, multiTableInit, "UPDATE a SET id = 1", 5000, TaskType.DML);

        assertTrue(result.success());
        assertEquals(2, result.rows().size());
        assertTrue(result.rows().stream().allMatch(r -> r.containsKey("_table")));
    }
}