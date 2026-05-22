package com.leetjourney.usage_service.controller;

import com.leetjourney.usage_service.dto.UsageDto;
import com.leetjourney.usage_service.service.PdfReportService;
import com.leetjourney.usage_service.service.UsageService;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/usage")
public class UsageController {

    private final UsageService usageService;
    private final PdfReportService pdfReportService;

    public UsageController(UsageService usageService,
                           PdfReportService pdfReportService) {
        this.usageService = usageService;
        this.pdfReportService = pdfReportService;
    }

    @GetMapping("/{userId}")
    public ResponseEntity<UsageDto> getUserDeviceUsage(
            @PathVariable Long userId,
            @RequestParam(defaultValue = "3") int days) {

        UsageDto usage = usageService.getXDaysUsageForUser(userId, days);
        return ResponseEntity.ok(usage);
    }

    @GetMapping("/report/{userId}")
    public ResponseEntity<byte[]> downloadReport(
            @PathVariable Long userId,
            @RequestParam(defaultValue = "7") int days
    ) {
        byte[] pdf = pdfReportService.generateUsageReport(userId, days);

        return ResponseEntity.ok()
                .header(
                        HttpHeaders.CONTENT_DISPOSITION,
                        "attachment; filename=energy-report-user-" + userId + ".pdf"
                )
                .contentType(MediaType.APPLICATION_PDF)
                .body(pdf);
    }
}