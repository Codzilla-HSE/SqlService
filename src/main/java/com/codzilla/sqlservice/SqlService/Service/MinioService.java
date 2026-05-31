package com.codzilla.sqlservice.SqlService.Service;

import io.minio.*;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Взаимодействие с MinIO (S3-совместимое хранилище).
 *
 * Хранит:
 * - init-скрипты задач: tasks/{taskId}/init.sql
 * - Java-валидаторы:    tasks/{taskId}/Validator.java
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MinioService {

    private final MinioClient minioClient;

    @Value("${minio.bucket}")
    private String bucket;

    @PostConstruct
    public void ensureBucketExists() {
        try {
            boolean found = minioClient.bucketExists(
                    BucketExistsArgs.builder().bucket(bucket).build()
            );
            if (!found) {
                minioClient.makeBucket(
                        MakeBucketArgs.builder().bucket(bucket).build()
                );
                log.info("Bucket '{}' created successfully", bucket);
            } else {
                log.info("Bucket '{}' already exists", bucket);
            }
        } catch (Exception e) {
            log.error("Cannot ensure bucket '{}': {}", bucket, e.getMessage());
            throw new IllegalStateException("MinIO bucket initialization failed", e);
        }
    }

    public String downloadAsString(String key) {
        try {
            InputStream stream = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(bucket)
                            .object(key)
                            .build()
            );
            String content = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            log.debug("Downloaded {} ({} bytes)", key, content.length());
            return content;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to download from MinIO: " + key, e);
        }
    }

    public void uploadString(String key, String content) {
        try {
            byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(bucket)
                            .object(key)
                            .stream(new ByteArrayInputStream(bytes), bytes.length, -1)
                            .contentType("text/plain")
                            .build()
            );
            log.info("Uploaded {} to MinIO ({} bytes)", key, bytes.length);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to upload to MinIO: " + key, e);
        }
    }
}