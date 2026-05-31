package com.codzilla.sqlservice.SqlService.Dto;

import java.io.Serializable;
import java.util.UUID;


public record SubmissionKafkaMessage(
        Long submissionId,
        Long taskId,
        UUID userId,
        String userSqlQuery
) implements Serializable {}