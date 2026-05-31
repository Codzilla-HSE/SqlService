package com.codzilla.sqlservice.SqlService.Dto;


import com.codzilla.sqlservice.SqlService.model.TaskType;
import jakarta.validation.constraints.Positive;

public record UpdateTaskRequest(
        String title,
        TaskType type,
        String description,
        String correctSqlQuery,
        String initSql,
        String validatorJavaCode,
        @Positive Integer timeLimitMs
) {}