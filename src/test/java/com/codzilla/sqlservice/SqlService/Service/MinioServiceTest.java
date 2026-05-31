package com.codzilla.sqlservice.SqlService.Service;

import io.minio.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.BDDMockito.*;

@ExtendWith(MockitoExtension.class)
class MinioServiceTest {

    @Mock private MinioClient minioClient;

    @InjectMocks
    private MinioService minioService;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(minioService, "bucket", "test-bucket");
    }

    @Test
    void ensureBucketExists_createsBucketIfNotExists() throws Exception {
        given(minioClient.bucketExists(any(BucketExistsArgs.class))).willReturn(false);

        minioService.ensureBucketExists();

        then(minioClient).should().makeBucket(any(MakeBucketArgs.class));
    }

    @Test
    void ensureBucketExists_doesNotCreateIfExists() throws Exception {
        given(minioClient.bucketExists(any(BucketExistsArgs.class))).willReturn(true);

        minioService.ensureBucketExists();

        then(minioClient).should(never()).makeBucket(any());
    }

    @Test
    void uploadString_success() throws Exception {
        minioService.uploadString("tasks/1/init.sql", "CREATE TABLE...");

        then(minioClient).should().putObject(any(PutObjectArgs.class));
    }

    @Test
    void uploadString_throwsIllegalStateExceptionOnError() throws Exception {
        given(minioClient.putObject(any(PutObjectArgs.class))).willThrow(new RuntimeException("MinIO unavailable"));

        assertThatThrownBy(() -> minioService.uploadString("tasks/1/init.sql", "CREATE TABLE..."))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to upload");
    }

    @Test
    void downloadAsString_success() throws Exception {
        GetObjectResponse response = mock(GetObjectResponse.class);
        given(response.readAllBytes()).willReturn("CONTENT".getBytes());
        given(minioClient.getObject(any(GetObjectArgs.class))).willReturn(response);

        String result = minioService.downloadAsString("tasks/1/init.sql");

        assertThat(result).isEqualTo("CONTENT");
    }

    @Test
    void downloadAsString_throwsIllegalStateExceptionOnError() throws Exception {
        given(minioClient.getObject(any(GetObjectArgs.class))).willThrow(new RuntimeException("MinIO not reachable"));

        assertThatThrownBy(() -> minioService.downloadAsString("tasks/1/init.sql"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to download");
    }
}