package com.codzilla.sqlservice.SqlService.Service;

import com.codzilla.sqlservice.SqlService.DB.*;
import com.codzilla.sqlservice.SqlService.Dto.CreateTaskRequest;
import com.codzilla.sqlservice.SqlService.Dto.UpdateTaskRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class TaskService {

    private final TaskRepository taskRepository;

    private final MinioService minioService;

    @Transactional
    public Task create(CreateTaskRequest req) {


        Task task = Task.builder()
                .title(req.title())
                .type(req.type())
                .description(req.description())
                .correctSqlResponse(req.correctSqlQuery())
                .timeLimitMs(req.timeLimitMs() != null ? req.timeLimitMs() : 30_000)
                .build();

        Task saved = taskRepository.save(task);

        if (req.initSql() != null && !req.initSql().isBlank()) {
            String key = "tasks/" + saved.getTaskId() + "/init.sql";
            minioService.uploadString(key, req.initSql());
            saved.setInitScriptKey(key);
            saved = taskRepository.save(saved);
        }

        if (req.validatorJavaCode() != null && !req.validatorJavaCode().isBlank()) {
            String key = "tasks/" + saved.getTaskId() + "/Validator.java";
            minioService.uploadString(key, req.validatorJavaCode());
            saved.setValidatorScriptKey(key);
            saved = taskRepository.save(saved);
        }

        log.info("Created task {} '{}' initKey={} validatorKey={}",
                saved.getTaskId(), saved.getTitle(),
                saved.getInitScriptKey(), saved.getValidatorScriptKey());
        return saved;
    }

    @Transactional(readOnly = true)
    public Task getById(Long taskId) {
        return taskRepository.findById(taskId)
                .orElseThrow(() -> new IllegalArgumentException("Task not found: " + taskId));
    }

    @Transactional(readOnly = true)
    public List<Task> getAll() {
        return taskRepository.findAll();
    }


    @Transactional
    public Task update(Long taskId, UpdateTaskRequest req) {
        Task task = taskRepository.findById(taskId)
                .orElseThrow(() -> new IllegalArgumentException("Task not found: " + taskId));

        if (req.title()          != null) task.setTitle(req.title());
        if (req.type()           != null) task.setType(req.type());
        if (req.description()    != null) task.setDescription(req.description());
        if (req.timeLimitMs()    != null) task.setTimeLimitMs(req.timeLimitMs());
        if (req.correctSqlQuery()!= null) task.setCorrectSqlResponse(req.correctSqlQuery());

        if (req.initSql() != null && !req.initSql().isBlank()) {
            String key = "tasks/" + taskId + "/init.sql";
            minioService.uploadString(key, req.initSql());
            task.setInitScriptKey(key);
        }

        if (req.validatorJavaCode() != null && !req.validatorJavaCode().isBlank()) {
            String key = "tasks/" + taskId + "/Validator.java";
            minioService.uploadString(key, req.validatorJavaCode());
            task.setValidatorScriptKey(key);
        }

        Task saved = taskRepository.save(task);
        log.info("Updated task {}", taskId);
        return saved;
    }

    @Transactional
    public void delete(Long taskId) {
        Task task = taskRepository.findById(taskId)
                .orElseThrow(() -> new IllegalArgumentException("Task not found: " + taskId));
        taskRepository.delete(task);
        log.info("Deleted task {}", taskId);
    }
}