package com.codzilla.sqlservice.SqlService.Service;

import com.codzilla.sqlservice.SqlService.validator.TaskValidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.tools.*;
import java.io.File;
import java.io.StringWriter;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Динамически компилирует и запускает Java-валидатор из MinIO.
 *
 * Поток:
 *   1. Получить Java-код валидатора как строку (из MinIO)
 *   2. Скомпилировать в памяти через javax.tools.JavaCompiler
 *   3. Загрузить класс через URLClassLoader
 *   4. Создать экземпляр и вызвать validate()
 *   5. Удалить временные файлы
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ValidatorScriptRunner {

    /**
     * Запустить валидатор.
     *
     * @param validatorJavaCode исходный код Java-класса (скачан из MinIO)
     * @param correctRows       результат правильного запроса
     * @param userRows          результат запроса пользователя
     * @return результат валидации
     */
    public ValidationResult run(String validatorJavaCode,
                                List<Map<String, Object>> correctRows,
                                List<Map<String, Object>> userRows) {
        Path tempDir = null;
        try {
            // 1. Создать temp-директорию для компиляции
            tempDir = Files.createTempDirectory("sql-validator-");
            String className = extractClassName(validatorJavaCode);
            Path javaFile = tempDir.resolve(className + ".java");
            Files.writeString(javaFile, validatorJavaCode);

            // 2. Скомпилировать
            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
            if (compiler == null) {
                throw new IllegalStateException("JavaCompiler not available (JDK required, not JRE)");
            }

            StringWriter compilerOutput = new StringWriter();
            try (StandardJavaFileManager fileManager =
                         compiler.getStandardFileManager(null, null, null)) {

                Iterable<? extends JavaFileObject> units =
                        fileManager.getJavaFileObjects(javaFile.toFile());

                boolean compiled = compiler
                        .getTask(compilerOutput, fileManager, null, null, null, units)
                        .call();

                if (!compiled) {
                    return ValidationResult.error(
                            "Validator compilation failed: " + compilerOutput);
                }
            }

            // 3. Загрузить класс
            URLClassLoader classLoader = URLClassLoader.newInstance(
                    new java.net.URL[]{tempDir.toUri().toURL()},
                    Thread.currentThread().getContextClassLoader()
            );

            Class<?> validatorClass = classLoader.loadClass(className);
            TaskValidator validator = (TaskValidator) validatorClass
                    .getDeclaredConstructor()
                    .newInstance();

            // 4. Запустить валидацию
            boolean accepted = validator.validate(correctRows, userRows);
            String message = accepted ? null : validator.failMessage(correctRows, userRows);

            log.debug("Validator {} result: {}", className, accepted);
            classLoader.close();
            return new ValidationResult(accepted, message, null);

        } catch (Exception e) {
            log.error("Validator execution error: {}", e.getMessage());
            return ValidationResult.error("Validator error: " + e.getMessage());
        } finally {
            // 5. Очистить temp файлы
            if (tempDir != null) {
                deleteDir(tempDir.toFile());
            }
        }
    }

    /**
     * Извлечь имя класса из исходного кода.
     * Ищет "public class ClassName" или "class ClassName".
     */
    private String extractClassName(String code) {
        for (String line : code.split("\n")) {
            line = line.trim();
            if (line.contains("class ")) {
                String[] parts = line.split("class ");
                if (parts.length > 1) {
                    return parts[1].split("[\\s{]")[0].trim();
                }
            }
        }
        throw new IllegalArgumentException("Cannot find class name in validator code");
    }

    private void deleteDir(File dir) {
        File[] files = dir.listFiles();
        if (files != null) for (File f : files) deleteDir(f);
        dir.delete();
    }

    public record ValidationResult(boolean accepted, String failMessage, String errorMessage) {
        public static ValidationResult error(String msg) {
            return new ValidationResult(false, null, msg);
        }
        public boolean hasError() { return errorMessage != null; }
    }
}