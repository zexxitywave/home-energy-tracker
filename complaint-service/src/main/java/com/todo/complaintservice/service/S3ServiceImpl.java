package com.todo.complaintservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class S3ServiceImpl implements S3Service {

    private final S3Client s3Client;

    @Value("${aws.s3.bucket-name}")
    private String bucketName;

    @Override
    public String uploadFile(MultipartFile file) {
        try {
            return uploadFile(
                    file.getOriginalFilename(),
                    file.getContentType(),
                    file.getBytes(),
                    null,
                    null
            );
        } catch (Exception e) {
            throw new RuntimeException("S3 upload failed: " + e.getMessage());
        }
    }

    @Override
    public String uploadFile(
            String originalFileName,
            String contentType,
            byte[] bytes,
            Long userId,
            Long complaintId
    ) {
        // S3 key format: users/{userId}/complaints/{complaintId}/{uuid}-{filename}
        String prefix = (userId != null && complaintId != null)
                ? "users/" + userId + "/complaints/" + complaintId + "/"
                : "uncategorized/";

        String fileName = prefix + UUID.randomUUID() + "-" + originalFileName;
        long startTime = System.currentTimeMillis();

        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(fileName)
                    .contentType(contentType)
                    .metadata(Map.of(
                            "uploaded-by", "home-energy-tracker",
                            "module", "complaint-service",
                            "original-file-name", originalFileName,
                            "user-id", userId != null ? String.valueOf(userId) : "unknown",
                            "complaint-id", complaintId != null ? String.valueOf(complaintId) : "unknown"
                    ))
                    .build();

            s3Client.putObject(putObjectRequest, RequestBody.fromBytes(bytes));

            long duration = System.currentTimeMillis() - startTime;
            log.info("✅ S3 upload success | key: {} | size: {} bytes | userId: {} | complaintId: {} | duration: {}ms",
                    fileName, bytes.length, userId, complaintId, duration);

            return fileName;

        } catch (Exception e) {
            log.error("❌ S3 upload failed | file: {} | userId: {} | error: {}", fileName, userId, e.getMessage());
            return null;
        }
    }
}
