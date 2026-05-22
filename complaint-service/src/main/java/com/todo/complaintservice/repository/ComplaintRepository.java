package com.todo.complaintservice.repository;

import com.todo.complaintservice.entity.Complaint;
import com.todo.complaintservice.enums.ComplaintStatus;
import com.todo.complaintservice.enums.Priority;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ComplaintRepository extends JpaRepository<Complaint, Long> {

    List<Complaint> findByUserId(Long userId);

    List<Complaint> findByStatus(ComplaintStatus status);

    List<Complaint> findByPriority(Priority priority);
}