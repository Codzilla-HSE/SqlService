package com.codzilla.sqlservice.SqlService.Service;

import com.codzilla.sqlservice.SqlService.DB.Task;
import com.codzilla.sqlservice.SqlService.DB.TaskRepository;
import com.codzilla.sqlservice.SqlService.Dto.CreateTaskRequest;
import com.codzilla.sqlservice.SqlService.Dto.UpdateTaskRequest;
import com.codzilla.sqlservice.SqlService.model.TaskType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.BDDMockito.*;

@ExtendWith(MockitoExtension.class)
class TaskServiceTest {

    @Mock private TaskRepository taskRepository;
    @Mock private MinioService minioService;

    @InjectMocks
    private TaskService taskService;

    private Task existingTask;
    private CreateTaskRequest createRequest;
    private UpdateTaskRequest updateRequest;

    @BeforeEach
    void setUp() {
        existingTask = Task.builder()
                .taskId(1L)
                .title("Test task")
                .type(TaskType.DQL)
                .description("desc")
                .correctSqlResponse("SELECT * FROM test")
                .timeLimitMs(10_000)
                .initScriptKey(null)
                .validatorScriptKey(null)
                .build();

        // Правильный порядок аргументов:
        // title, type, description, correctSqlQuery, initSql, validatorJavaCode, timeLimitMs
        createRequest = new CreateTaskRequest(
                "New task", TaskType.DQL, "new desc", "SELECT 1",
                "init sql content", "validator java code", 5000
        );

        updateRequest = new UpdateTaskRequest(
                "Updated title", null, null, null, null, null, null
        );
    }

    @Test
    void create_withInitAndValidator_uploadsBothAndReturnsSavedTask() {
        given(taskRepository.save(any(Task.class))).willAnswer(inv -> {
            Task t = inv.getArgument(0);
            if (t.getTaskId() == null) {
                t.setTaskId(1L);  // имитация генерации ID
            }
            return t;
        });

        Task result = taskService.create(createRequest);

        then(minioService).should().uploadString("tasks/1/init.sql", "init sql content");
        then(minioService).should().uploadString("tasks/1/Validator.java", "validator java code");
        assertThat(result.getTitle()).isEqualTo("New task");
        assertThat(result.getInitScriptKey()).isEqualTo("tasks/1/init.sql");
        assertThat(result.getValidatorScriptKey()).isEqualTo("tasks/1/Validator.java");
        assertThat(result.getTimeLimitMs()).isEqualTo(5000);
    }

    @Test
    void create_withoutInitAndValidator_doesNotUpload() {
        CreateTaskRequest req = new CreateTaskRequest(
                "Task", TaskType.DML, "desc", "UPDATE users SET x=1",
                null, null, 3000   // initSql=null, validatorJavaCode=null, timeLimitMs=3000
        );

        given(taskRepository.save(any(Task.class))).willAnswer(inv -> {
            Task t = inv.getArgument(0);
            t.setTaskId(2L);
            return t;
        });

        Task result = taskService.create(req);

        then(minioService).should(never()).uploadString(anyString(), anyString());
        assertThat(result.getInitScriptKey()).isNull();
        assertThat(result.getValidatorScriptKey()).isNull();
        assertThat(result.getTimeLimitMs()).isEqualTo(3000);
    }

    @Test
    void create_withDefaultTimeLimitIfNull() {
        CreateTaskRequest req = new CreateTaskRequest(
                "Task", TaskType.DQL, "desc", "SELECT 1",
                null, null, null   // timeLimitMs=null → дефолт 30_000
        );

        given(taskRepository.save(any(Task.class))).willAnswer(inv -> {
            Task t = inv.getArgument(0);
            t.setTaskId(3L);
            return t;
        });

        Task result = taskService.create(req);
        assertThat(result.getTimeLimitMs()).isEqualTo(30_000);
    }

    @Test
    void getAll_returnsAllTasks() {
        given(taskRepository.findAll()).willReturn(List.of(existingTask));

        List<Task> tasks = taskService.getAll();
        assertThat(tasks).hasSize(1);
        assertThat(tasks.get(0).getTitle()).isEqualTo("Test task");
    }

    @Test
    void getById_existingId_returnsTask() {
        given(taskRepository.findById(1L)).willReturn(Optional.of(existingTask));

        Task result = taskService.getById(1L);
        assertThat(result).isEqualTo(existingTask);
    }

    @Test
    void getById_nonExistingId_throwsException() {
        given(taskRepository.findById(99L)).willReturn(Optional.empty());

        assertThatThrownBy(() -> taskService.getById(99L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Task not found");
    }

    @Test
    void update_updatesFieldsAndUploadsScripts() {
        given(taskRepository.findById(1L)).willReturn(Optional.of(existingTask));
        given(taskRepository.save(any(Task.class))).willReturn(existingTask);

        // title, type, description, correctSqlQuery, initSql, validatorJavaCode, timeLimitMs
        UpdateTaskRequest req = new UpdateTaskRequest(
                "Updated", TaskType.DML, "new desc", "DELETE FROM x",
                "new init", "new validator", 15000
        );

        Task updated = taskService.update(1L, req);

        assertThat(updated.getTitle()).isEqualTo("Updated");
        assertThat(updated.getType()).isEqualTo(TaskType.DML);
        assertThat(updated.getTimeLimitMs()).isEqualTo(15000);
        then(minioService).should().uploadString("tasks/1/init.sql", "new init");
        then(minioService).should().uploadString("tasks/1/Validator.java", "new validator");
    }

    @Test
    void update_onlyProvidedFields() {
        given(taskRepository.findById(1L)).willReturn(Optional.of(existingTask));
        given(taskRepository.save(any(Task.class))).willReturn(existingTask);

        // Передаём только title, остальное null
        Task updated = taskService.update(1L, updateRequest);

        assertThat(updated.getTitle()).isEqualTo("Updated title");
        assertThat(updated.getType()).isEqualTo(TaskType.DQL); // не изменилось
        then(minioService).should(never()).uploadString(anyString(), anyString());
    }

    @Test
    void update_nonExistingId_throwsException() {
        given(taskRepository.findById(99L)).willReturn(Optional.empty());

        assertThatThrownBy(() -> taskService.update(99L, updateRequest))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Task not found");
    }

    @Test
    void delete_existingId_deletesTask() {
        given(taskRepository.findById(1L)).willReturn(Optional.of(existingTask));
        taskService.delete(1L);
        then(taskRepository).should().delete(existingTask);
    }

    @Test
    void delete_nonExistingId_throwsException() {
        given(taskRepository.findById(99L)).willReturn(Optional.empty());
        assertThatThrownBy(() -> taskService.delete(99L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Task not found");
    }
}