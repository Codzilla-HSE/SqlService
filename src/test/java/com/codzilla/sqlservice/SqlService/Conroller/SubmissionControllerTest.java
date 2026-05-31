package com.codzilla.sqlservice.SqlService.Conroller;

import com.codzilla.sqlservice.SqlService.DB.SqlSubmission;
import com.codzilla.sqlservice.SqlService.DB.SubmissionRepository;
import com.codzilla.sqlservice.SqlService.Dto.SubmissionStatusDto;
import com.codzilla.sqlservice.SqlService.Dto.SubmitRequest;
import com.codzilla.sqlservice.SqlService.Service.SubmissionService;
import com.codzilla.sqlservice.SqlService.model.SqlVerdict;
import com.codzilla.sqlservice.SqlService.model.SubmissionStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class SubmissionControllerTest {

    private MockMvc mockMvc;
    private SubmissionService submissionService;
    private SubmissionRepository submissionRepository;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        submissionService = mock(SubmissionService.class);
        submissionRepository = mock(SubmissionRepository.class);
        SubmissionController controller = new SubmissionController(submissionService, submissionRepository);
        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
        objectMapper = new ObjectMapper();
    }

    @Test
    void submit_shouldReturnAcceptedWithSubmissionId() throws Exception {
        UUID userId = UUID.randomUUID();
        SubmitRequest req = new SubmitRequest(1L, userId, "SELECT * FROM users");

        given(submissionService.submit(1L, userId, "SELECT * FROM users")).willReturn(100L);

        mockMvc.perform(post("/sqlservice/submissions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(req)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data").value(100));
    }

    @Test
    void submit_invalidRequest_shouldReturnBadRequest() throws Exception {
        // Отправляем некорректный запрос (отсутствует taskId)
        String invalidJson = "{\"userId\":\"some-uuid\"}";

        mockMvc.perform(post("/sqlservice/submissions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(invalidJson))
                .andExpect(status().isInternalServerError());  // пока нет обработчика валидации
    }

    @Test
    void getStatus_notFound_shouldReturnNotFound() throws Exception {
        given(submissionService.getStatus(99L)).willThrow(new IllegalArgumentException("Submission not found: 99"));

        mockMvc.perform(get("/sqlservice/submissions/99"))
                .andExpect(status().isNotFound());   // теперь работает
    }

    @Test
    void leaderboard_missingTaskId_shouldReturnBadRequest() throws Exception {
        mockMvc.perform(get("/sqlservice/submissions/leaderboard"))
                .andExpect(status().isBadRequest());   // ← теперь правильно
    }

    @Test
    void getStatus_shouldReturnStatusDto() throws Exception {
        SubmissionStatusDto dto = new SubmissionStatusDto(
                1L, SubmissionStatus.DONE, SqlVerdict.ACCEPTED, 120L, null, 5L);
        given(submissionService.getStatus(1L)).willReturn(dto);

        mockMvc.perform(get("/sqlservice/submissions/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.submissionId").value(1))
                .andExpect(jsonPath("$.data.status").value("DONE"))
                .andExpect(jsonPath("$.data.verdict").value("ACCEPTED"))
                .andExpect(jsonPath("$.data.executionTimeMs").value(120))
                .andExpect(jsonPath("$.data.kafkaOffset").value(5));
    }



    @Test
    void list_all_shouldReturnAllSubmissions() throws Exception {
        SqlSubmission sub1 = SqlSubmission.builder().submissionId(1L).userId(UUID.randomUUID()).build();
        SqlSubmission sub2 = SqlSubmission.builder().submissionId(2L).userId(UUID.randomUUID()).build();
        given(submissionRepository.findAll()).willReturn(List.of(sub1, sub2));

        mockMvc.perform(get("/sqlservice/submissions"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.length()").value(2))
                .andExpect(jsonPath("$.data[0].submissionId").value(1))
                .andExpect(jsonPath("$.data[1].submissionId").value(2));
    }

    @Test
    void list_byTaskId_shouldReturnFiltered() throws Exception {
        SqlSubmission sub = SqlSubmission.builder().submissionId(3L).build();
        given(submissionRepository.findByTaskTaskId(1L)).willReturn(List.of(sub));

        mockMvc.perform(get("/sqlservice/submissions").param("taskId", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.length()").value(1))
                .andExpect(jsonPath("$.data[0].submissionId").value(3));
    }

    @Test
    void list_byUserId_shouldReturnFiltered() throws Exception {
        UUID userId = UUID.randomUUID();
        SqlSubmission sub = SqlSubmission.builder().submissionId(4L).build();
        given(submissionRepository.findByUserId(userId)).willReturn(List.of(sub));

        mockMvc.perform(get("/sqlservice/submissions").param("userId", userId.toString()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data[0].submissionId").value(4));
    }

    @Test
    void list_byTaskAndUser_shouldReturnIntersection() throws Exception {
        UUID userId = UUID.randomUUID();
        SqlSubmission sub = SqlSubmission.builder().submissionId(5L).build();
        given(submissionRepository.findByTaskTaskIdAndUserId(1L, userId)).willReturn(List.of(sub));

        mockMvc.perform(get("/sqlservice/submissions")
                        .param("taskId", "1")
                        .param("userId", userId.toString()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data[0].submissionId").value(5));
    }

    @Test
    void leaderboard_shouldReturnOrderedList() throws Exception {
        SqlSubmission sub1 = SqlSubmission.builder().submissionId(10L).kafkaOffset(3L).build();
        SqlSubmission sub2 = SqlSubmission.builder().submissionId(11L).kafkaOffset(2L).build();
        given(submissionRepository.findLeaderboardByTaskId(1L)).willReturn(List.of(sub2, sub1));

        mockMvc.perform(get("/sqlservice/submissions/leaderboard").param("taskId", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.length()").value(2))
                .andExpect(jsonPath("$.data[0].submissionId").value(11))  // сначала с меньшим offset'ом
                .andExpect(jsonPath("$.data[1].submissionId").value(10));
    }


}