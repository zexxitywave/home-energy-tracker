package com.todo.complaintservice.service;

import org.springframework.web.multipart.MultipartFile;

public interface S3Service {

    String uploadFile(MultipartFile file);

    String uploadFile(
            String fileName,
            String contentType,
            byte[] bytes
    );
}