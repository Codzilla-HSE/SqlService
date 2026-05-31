package com.codzilla.sqlservice.SqlService.DB;

import com.codzilla.sqlservice.SqlService.model.SqlVerdict;
import com.codzilla.sqlservice.SqlService.model.SubmissionStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "sql_submissions")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SqlSubmission {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long submissionId;

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "task_id", nullable = false)
    private Task task;

    @Column(nullable = false)
    private UUID userId;

    @Column(columnDefinition = "TEXT", nullable = false)
    private String userSqlQuery;

    @Column(nullable = false)
    @Enumerated(EnumType.STRING)
    private SubmissionStatus status;

    @Enumerated(EnumType.STRING)
    private SqlVerdict verdict;

    @Column(name = "kafka_offset")
    private Long kafkaOffset;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "rows_matched")
    private Boolean rowsMatched;

    @Column(name = "execution_time_ms")
    private Long executionTimeMs;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
    }
}