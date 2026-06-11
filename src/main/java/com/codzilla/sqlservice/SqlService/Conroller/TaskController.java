package com.codzilla.sqlservice.SqlService.Conroller;



import com.codzilla.sqlservice.SqlService.DB.Task;
import com.codzilla.sqlservice.SqlService.Service.TaskService;
import com.codzilla.sqlservice.SqlService.Dto.ApiResponse;
import com.codzilla.sqlservice.SqlService.Dto.CreateTaskRequest;
import com.codzilla.sqlservice.SqlService.Dto.UpdateTaskRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/sqlservice/tasks")
@RequiredArgsConstructor
public class TaskController {

    private final TaskService taskService;

    /**
     * POST /sqlservice/tasks
     * Создать новую задачу.
     */
    @PostMapping
    public ResponseEntity<ApiResponse<Task>> create(
            @Valid @RequestBody CreateTaskRequest req
    ) {
        Task task = taskService.create(req);
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(ApiResponse.ok(task));
    }

    /**
     * GET /sqlservice/tasks/{id}
     * Получить задачу по id (для отображения условия пользователю).
     */
    @GetMapping("/{id}")
    public ResponseEntity<ApiResponse<Task>> getById(@PathVariable Long id) {
        return ResponseEntity.ok(ApiResponse.ok(taskService.getById(id)));
    }

    /**
     * GET /sqlservice/tasks
     * GET /sqlservice/tasks?databaseId=1
     * Список всех задач или задач конкретной БД.
     */
    @GetMapping
    public ResponseEntity<ApiResponse<List<Task>>> list() {
        List<Task> tasks = taskService.getAll();
        return ResponseEntity.ok(ApiResponse.ok(tasks));
    }

    /**
     * PATCH /sqlservice/tasks/{id}
     * Частичное обновление задачи. null-поля игнорируются.
     * При смене correct_sql — кеш инвалидируется автоматически в TaskService.
     */
    @PatchMapping("/{id}")
    public ResponseEntity<ApiResponse<Task>> update(
            @PathVariable Long id,
            @Valid @RequestBody UpdateTaskRequest req
    ) {
        Task updated = taskService.update(id, req);
        return ResponseEntity.ok(ApiResponse.ok(updated));
    }

    /**
     * DELETE /sqlservice/tasks/{id}
     * Удалить задачу (каскадно удаляет кеш).
     * Посылки сохраняются для истории.
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<ApiResponse<Void>> delete(@PathVariable Long id) {
        taskService.delete(id);
        return ResponseEntity.ok(ApiResponse.ok(null));
    }

    @GetMapping(params = "complexity")
    public ResponseEntity<ApiResponse<List<Task>>> getByComplexity(
            @RequestParam Task.TaskComplexity complexity) {
        return ResponseEntity.ok(new ApiResponse<>(true, taskService.getAllByComplexity(complexity), null));
    }
}