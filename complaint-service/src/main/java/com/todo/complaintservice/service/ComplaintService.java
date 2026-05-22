package com.todo.complaintservice.service;

import com.todo.complaintservice.entity.Complaint;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

public interface ComplaintService {

    Complaint createComplaint(
            Complaint complaint,
            List<MultipartFile> files
    );

    Complaint getComplaintById(Long id);

    List<Complaint> getComplaintsByUser(Long userId);

    List<Complaint> getAllComplaints();

    Complaint updateComplaintStatus(Long id, String status);

    void deleteComplaint(Long id);
}