package com.todo.complaintservice.service;

import com.todo.complaintservice.entity.Complaint;
import com.todo.complaintservice.enums.ComplaintStatus;
import com.todo.complaintservice.repository.ComplaintRepository;

import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

@Service
public class ComplaintServiceImpl implements ComplaintService {

    private final ComplaintRepository complaintRepository;
    private final S3Service s3Service;

    public ComplaintServiceImpl(
            ComplaintRepository complaintRepository,
            S3Service s3Service
    ) {
        this.complaintRepository = complaintRepository;
        this.s3Service = s3Service;
    }

    @Override
    public Complaint createComplaint(
            Complaint complaint,
            List<MultipartFile> files
    ) {

        complaint.setStatus(ComplaintStatus.OPEN);

        Complaint savedComplaint =
                complaintRepository.save(complaint);

        if (files != null && !files.isEmpty()) {

            List<FileUploadData> uploadFiles = files.stream()
                    .filter(file -> file != null && !file.isEmpty())
                    .map(file -> {
                        try {
                            return new FileUploadData(
                                    file.getOriginalFilename(),
                                    file.getContentType(),
                                    file.getBytes()
                            );
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .toList();

            CompletableFuture.runAsync(() -> {

                List<String> imageKeys = uploadFiles.stream()
                        .map(fileData ->
                                s3Service.uploadFile(
                                        fileData.fileName(),
                                        fileData.contentType(),
                                        fileData.bytes()
                                )
                        )
                        .filter(Objects::nonNull)
                        .toList();

                if (!imageKeys.isEmpty()) {
                    savedComplaint.setImageKeys(
                            String.join(",", imageKeys)
                    );
                    complaintRepository.save(savedComplaint);
                }
            });
        }

        return savedComplaint;
    }

    @Override
    public Complaint getComplaintById(Long id) {
        return complaintRepository.findById(id)
                .orElseThrow(() ->
                        new RuntimeException("Complaint not found"));
    }

    @Override
    public List<Complaint> getComplaintsByUser(Long userId) {
        return complaintRepository.findByUserId(userId);
    }

    @Override
    public List<Complaint> getAllComplaints() {
        return complaintRepository.findAll();
    }

    @Override
    public Complaint updateComplaintStatus(Long id, String status) {
        Complaint complaint = getComplaintById(id);

        complaint.setStatus(
                ComplaintStatus.valueOf(status.toUpperCase())
        );

        return complaintRepository.save(complaint);
    }

    @Override
    public void deleteComplaint(Long id) {
        complaintRepository.deleteById(id);
    }
}