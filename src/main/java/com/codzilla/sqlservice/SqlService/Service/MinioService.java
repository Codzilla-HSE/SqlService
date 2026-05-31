package com.codzilla.sqlservice.SqlService.Service;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
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

    /**
     * Скачать файл из MinIO как строку.
     * @param key путь вида "tasks/1/init.sql"
     */
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

    /**
     * Загрузить текстовый файл в MinIO.
     * Используется при создании задачи (через UI/API).
     */
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