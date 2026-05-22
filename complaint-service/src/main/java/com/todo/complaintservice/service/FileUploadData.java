package com.todo.complaintservice.service;

public record FileUploadData(
        String fileName,
        String contentType,
        byte[] bytes
) {
}