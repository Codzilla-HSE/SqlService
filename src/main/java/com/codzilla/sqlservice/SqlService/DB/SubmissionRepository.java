package com.codzilla.sqlservice.SqlService.DB;

import com.codzilla.sqlservice.SqlService.model.SubmissionStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface SubmissionRepository extends JpaRepository<SqlSubmission, Long> {

    List<SqlSubmission> findByUserId(UUID userId);

    List<SqlSubmission> findByTaskTaskId(Long taskId);

    List<SqlSubmission> findByTaskTaskIdAndUserId(Long taskId, UUID userId);

    List<SqlSubmission> findByStatus(SubmissionStatus status);

    @Query("""
        SELECT s FROM SqlSubmission s
        WHERE s.task.taskId = :taskId
        ORDER BY
            CASE WHEN s.verdict = 'ACCEPTED' THEN 0 ELSE 1 END,
            s.kafkaOffset ASC NULLS LAST,
            s.createdAt ASC
    """)
    List<SqlSubmission> findLeaderboardByTaskId(@Param("taskId") Long taskId);
}