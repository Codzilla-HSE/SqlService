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
        String   initSql,
        String   validatorJavaCode,
        @Positive Integer timeLimitMs
) {}