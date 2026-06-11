package com.codzilla.sqlservice.SqlService.DB;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

import java.util.Optional;

@Repository
public interface TaskRepository extends JpaRepository<Task, Long> {
    List<Task> findAllByComplexity(Task.TaskComplexity complexity);

    @Query(value = "SELECT * FROM tasks WHERE complexity = :complexity ORDER BY RANDOM() LIMIT 1", nativeQuery = true)
    Optional<Task> findRandomByComplexity(@Param("complexity") String complexity);
}