package com.todo.complaintservice.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.util.Map;
import java.util.UUID;

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
                    file.getBytes()
            );

        } catch (Exception e) {
            throw new RuntimeException(
                    "S3 upload failed: " + e.getMessage()
            );
        }
    }

    @Override
    public String uploadFile(
            String originalFileName,
            String contentType,
            byte[] bytes
    ) {

        String fileName =
                UUID.randomUUID() + "-" + originalFileName;

        try {

            PutObjectRequest putObjectRequest =
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(fileName)
                            .contentType(contentType)
                            .metadata(
                                    Map.of(
                                            "uploaded-by", "home-energy-tracker",
                                            "module", "complaint-service",
                                            "original-file-name", originalFileName
                                    )
                            )
                            .build();

            s3Client.putObject(
                    putObjectRequest,
                    RequestBody.fromBytes(bytes)
            );

            System.out.println("S3 upload success: " + fileName);

            return fileName;

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}