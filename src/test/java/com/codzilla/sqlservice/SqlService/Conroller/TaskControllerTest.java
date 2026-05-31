package com.codzilla.sqlservice.SqlService.Conroller;

import com.codzilla.sqlservice.SqlService.DB.Task;
import com.codzilla.sqlservice.SqlService.Dto.CreateTaskRequest;
import com.codzilla.sqlservice.SqlService.Dto.UpdateTaskRequest;
import com.codzilla.sqlservice.SqlService.Service.TaskService;
import com.codzilla.sqlservice.SqlService.model.TaskType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.LocalDateTime;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class TaskControllerTest {

    private MockMvc mockMvc;
    private TaskService taskService;
    private ObjectMapper objectMapper;

    private final Task sampleTask = Task.builder()
            .taskId(1L)
            .title("Test task")
            .type(TaskType.DQL)
            .description("desc")
            .correctSqlResponse("SELECT 1")
            .timeLimitMs(5000)
            .createdAt(LocalDateTime.now())
            .build();

    @BeforeEach
    void setUp() {
        taskService = mock(TaskService.class);
        TaskController controller = new TaskController(taskService);
        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler()) 
                .build();
        objectMapper = new ObjectMapper();
    }

    @Test
    void create_shouldReturnCreatedWithTask() throws Exception {
        CreateTaskRequest req = new CreateTaskRequest(
                "New", TaskType.DML, "desc", "UPDATE users SET x=1",
                null, null, 3000
        );

        given(taskService.create(any(CreateTaskRequest.class))).willReturn(sampleTask);

        mockMvc.perform(post("/sqlservice/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(req)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.taskId").value(1))
                .andExpect(jsonPath("$.data.title").value("Test task"));
    }

    @Test
    void getById_shouldReturnTask() throws Exception {
        given(taskService.getById(1L)).willReturn(sampleTask);

        mockMvc.perform(get("/sqlservice/tasks/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.taskId").value(1));
    }


    @Test
    void list_shouldReturnAllTasks() throws Exception {
        Task task2 = Task.builder().taskId(2L).title("Second").type(TaskType.DQL).build();
        given(taskService.getAll()).willReturn(List.of(sampleTask, task2));

        mockMvc.perform(get("/sqlservice/tasks"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.length()").value(2))
                .andExpect(jsonPath("$.data[0].taskId").value(1))
                .andExpect(jsonPath("$.data[1].taskId").value(2));
    }

    @Test
    void update_shouldReturnUpdatedTask() throws Exception {
        UpdateTaskRequest req = new UpdateTaskRequest(
                "Updated title", null, null, null, null, null, null
        );

        given(taskService.update(eq(1L), any(UpdateTaskRequest.class))).willReturn(sampleTask);

        mockMvc.perform(patch("/sqlservice/tasks/1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(req)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.title").value("Test task"));
    }



    @Test
    void delete_shouldReturnOk() throws Exception {
        willDoNothing().given(taskService).delete(1L);

        mockMvc.perform(delete("/sqlservice/tasks/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true));
    }
    @Test
    void getById_notFound_shouldReturnNotFound() throws Exception {
        given(taskService.getById(99L)).willThrow(new IllegalArgumentException("Task not found: 99"));

        mockMvc.perform(get("/sqlservice/tasks/99"))
                .andExpect(status().isNotFound());   
    }

    @Test
    void update_notFound_shouldReturnNotFound() throws Exception {
        UpdateTaskRequest req = new UpdateTaskRequest(null, null, null, null, null, null, null);
        given(taskService.update(eq(99L), any())).willThrow(new IllegalArgumentException("Task not found: 99"));

        mockMvc.perform(patch("/sqlservice/tasks/99")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(req)))
                .andExpect(status().isNotFound());
    }

    @Test
    void delete_notFound_shouldReturnNotFound() throws Exception {
        willThrow(new IllegalArgumentException("Task not found: 99")).given(taskService).delete(99L);

        mockMvc.perform(delete("/sqlservice/tasks/99"))
                .andExpect(status().isNotFound());
    }
}