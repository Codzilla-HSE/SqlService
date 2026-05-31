package com.codzilla.sqlservice.SqlService.Service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class ValidatorScriptRunnerTest {

    private ValidatorScriptRunner runner;

    private final String validValidatorCode = """
        import com.codzilla.sqlservice.SqlService.validator.TaskValidator;
        import java.util.*;

        public class TestValidator implements TaskValidator {

            @Override
            public boolean validate(List<Map<String, Object>> correct,
                                    List<Map<String, Object>> user) {
                if (correct.size() != user.size()) return false;

                // Сравниваем только поле name
                List<String> correctNames = correct.stream()
                        .map(r -> (String) r.get("name"))
                        .sorted().toList();
                List<String> userNames = user.stream()
                        .map(r -> (String) r.get("name"))
                        .sorted().toList();
                return correctNames.equals(userNames);
            }

            @Override
            public String failMessage(List<Map<String, Object>> correct,
                                      List<Map<String, Object>> user) {
                return "Expected " + correct.size() + " rows, got " + user.size();
            }
        }
        """;

    private final String failingValidatorCode = """
        import com.codzilla.sqlservice.SqlService.validator.TaskValidator;
        import java.util.*;

        public class AlwaysFailValidator implements TaskValidator {

            @Override
            public boolean validate(List<Map<String, Object>> correct,
                                    List<Map<String, Object>> user) {
                return false;
            }

            @Override
            public String failMessage(List<Map<String, Object>> correct,
                                      List<Map<String, Object>> user) {
                return "Always fail";
            }
        }
        """;

    private final String invalidCode = """
        public class BrokenValidator {
            // not implementing TaskValidator
        }
        """;

    @BeforeEach
    void setUp() {
        runner = new ValidatorScriptRunner();
    }

    @Test
    void run_withValidValidator_shouldReturnAccepted() {
        List<Map<String, Object>> correct = List.of(
                Map.of("name", "Alice"), Map.of("name", "Bob")
        );
        List<Map<String, Object>> user = List.of(
                Map.of("name", "Bob"), Map.of("name", "Alice")
        );

        ValidatorScriptRunner.ValidationResult result = runner.run(validValidatorCode, correct, user);

        assertThat(result.accepted()).isTrue();
        assertThat(result.hasError()).isFalse();
        assertThat(result.failMessage()).isNull();
    }

    @Test
    void run_withValidValidator_whenMismatch_shouldReturnWrongAnswer() {
        List<Map<String, Object>> correct = List.of(Map.of("name", "Alice"));
        List<Map<String, Object>> user = List.of(Map.of("name", "Bob"));

        ValidatorScriptRunner.ValidationResult result = runner.run(validValidatorCode, correct, user);

        assertThat(result.accepted()).isFalse();
        assertThat(result.hasError()).isFalse();
        assertThat(result.failMessage()).contains("Expected 1 rows, got 1");
    }

    @Test
    void run_withAlwaysFailValidator_shouldReturnNotAcceptedWithMessage() {
        List<Map<String, Object>> correct = List.of(Map.of("a", 1));
        List<Map<String, Object>> user = List.of(Map.of("a", 1));

        ValidatorScriptRunner.ValidationResult result = runner.run(failingValidatorCode, correct, user);

        assertThat(result.accepted()).isFalse();
        assertThat(result.hasError()).isFalse();
        assertThat(result.failMessage()).isEqualTo("Always fail");
    }

    @Test
    void run_withInvalidCode_shouldReturnErrorResult() {
        List<Map<String, Object>> correct = List.of();
        List<Map<String, Object>> user = List.of();

        ValidatorScriptRunner.ValidationResult result = runner.run(invalidCode, correct, user);

        assertThat(result.accepted()).isFalse();
        assertThat(result.hasError()).isTrue();
        assertThat(result.errorMessage()).contains("Validator error");
    }

    @Test
    void run_withMissingClassName_shouldReturnErrorResult() {
        String noClassCode = "// no class definition\nSystem.out.println();";

        ValidatorScriptRunner.ValidationResult result = runner.run(noClassCode, List.of(), List.of());

        assertThat(result.hasError()).isTrue();
    }
}