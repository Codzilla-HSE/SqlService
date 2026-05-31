package com.codzilla.sqlservice.SqlService.validator;

import java.util.List;
import java.util.Map;

/**
 * Интерфейс который реализует составитель задачи.
 *
 * Пример реализации для задачи:
 *
 * public class Task1Validator implements TaskValidator {
 *     @Override
 *     public boolean validate(List<Map<String, Object>> correct,
 *                             List<Map<String, Object>> user) {
 *         // Своя логика: порядок важен, или нет, или проверка подмножества
 *         return correct.equals(user);
 *     }
 *
 *     @Override
 *     public String failMessage(List<Map<String, Object>> correct,
 *                               List<Map<String, Object>> user) {
 *         return "Expected " + correct.size() + " rows, got " + user.size();
 *     }
 * }
 *
 * Этот файл компилируется и загружается динамически из MinIO.
 */
public interface TaskValidator {

    /**
     * Проверить правильность ответа пользователя.
     * @param correctRows результат выполнения correct_sql
     * @param userRows    результат выполнения user_sql
     * @return true = ACCEPTED
     */
    boolean validate(List<Map<String, Object>> correctRows,
                     List<Map<String, Object>> userRows);

    /**
     * Сообщение при неправильном ответе (опционально).
     */
    default String failMessage(List<Map<String, Object>> correctRows,
                               List<Map<String, Object>> userRows) {
        return "Wrong answer";
    }
}