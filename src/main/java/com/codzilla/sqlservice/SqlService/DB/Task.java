package com.codzilla.sqlservice.SqlService.DB;

import com.codzilla.sqlservice.SqlService.model.TaskType;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "tasks")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Task {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long taskId;

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "database_id", nullable = false)
    private DatabaseEntity database;

    @Column(name = "title", nullable = false)
    private String title;

    @Column(name = "task_type", nullable = false)
    @Enumerated(EnumType.STRING)
    private TaskType type;

    @Column(name = "description", columnDefinition = "TEXT")
    private String description;

    @Column(name = "correct_sql", nullable = false, columnDefinition = "TEXT")
    private String correctSqlResponse;

    /**
     * Путь к init-скрипту в MinIO: "bucket/tasks/{taskId}/init.sql"
     * Скрипт создаёт таблицы и наполняет их данными для этой задачи.
     */
    @Column(name = "init_script_key")
    private String initScriptKey;

    /**
     * Путь к Java-валидатору в MinIO: "bucket/tasks/{taskId}/Validator.java"
     * Опционально — если null, используется дефолтное сравнение результатов.
     * Валидатор реализует интерфейс TaskValidator.
     */
    @Column(name = "validator_script_key")
    private String validatorScriptKey;

    @Column(name = "time_limit_ms")
    private Integer timeLimitMs;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
    }
}