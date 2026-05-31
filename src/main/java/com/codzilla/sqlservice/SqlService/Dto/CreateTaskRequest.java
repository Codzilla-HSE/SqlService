package com.codzilla.sqlservice.SqlService.Dto;

import com.codzilla.sqlservice.SqlService.model.TaskType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record CreateTaskRequest(
        @NotBlank String   title,
        @NotNull  TaskType type,
        String   description,
        @NotBlank String   correctSqlQuery,
        // init.sql — создаёт таблицы и данные для задачи
        String   initSql,
        // Java-код валидатора (опционально)
        String   validatorJavaCode,
        @Positive Integer timeLimitMs
) {}