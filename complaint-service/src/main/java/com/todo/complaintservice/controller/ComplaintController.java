package com.todo.complaintservice.controller;
import java.util.List;
import org.springframework.web.multipart.MultipartFile;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.todo.complaintservice.entity.Complaint;
import com.todo.complaintservice.service.ComplaintService;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
@CrossOrigin(origins = "*")
@RestController
@RequestMapping("/api/complaints")
public class ComplaintController {

    private final ComplaintService complaintService;

    public ComplaintController(ComplaintService complaintService) {
        this.complaintService = complaintService;
    }

    @PostMapping(consumes = "multipart/form-data")
    public ResponseEntity<Complaint> createComplaint(

            @RequestPart("complaint")
            String complaintJson,

            @RequestPart(value = "files", required = false)
            List<MultipartFile> files

    ) throws Exception {

        System.out.println("Files received in controller: " +
                (files == null ? 0 : files.size()));

        ObjectMapper objectMapper = new ObjectMapper();

        Complaint complaint =
                objectMapper.readValue(
                        complaintJson,
                        Complaint.class
                );

        return ResponseEntity.ok(
                complaintService.createComplaint(
                        complaint,
                        files
                )
        );
    }

    @GetMapping("/{id}")
    public ResponseEntity<Complaint> getComplaintById(
            @PathVariable Long id
    ) {

        return ResponseEntity.ok(
                complaintService.getComplaintById(id)
        );
    }

    @GetMapping("/user/{userId}")
    public ResponseEntity<List<Complaint>> getComplaintsByUser(
            @PathVariable Long userId
    ) {

        return ResponseEntity.ok(
                complaintService.getComplaintsByUser(userId)
        );
    }

    @GetMapping
    public ResponseEntity<List<Complaint>> getAllComplaints() {

        return ResponseEntity.ok(
                complaintService.getAllComplaints()
        );
    }

    @PutMapping("/{id}/status")
    public ResponseEntity<Complaint> updateComplaintStatus(

            @PathVariable Long id,
            @RequestParam String status
    ) {

        return ResponseEntity.ok(
                complaintService.updateComplaintStatus(id, status)
        );
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<String> deleteComplaint(
            @PathVariable Long id
    ) {

        complaintService.deleteComplaint(id);

        return ResponseEntity.ok(
                "Complaint deleted successfully"
        );
    }
}