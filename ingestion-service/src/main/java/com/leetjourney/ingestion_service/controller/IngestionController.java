package com.leetjourney.ingestion_service.controller;

import com.leetjourney.ingestion_service.dto.EnergyUsageDto;
import com.leetjourney.ingestion_service.service.IngestionService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/ingestion")
public class IngestionController {

    private final IngestionService ingestionService;

    public IngestionController(IngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @PostMapping
    @ResponseStatus(org.springframework.http.HttpStatus.CREATED)
    public void ingestData(@RequestBody EnergyUsageDto usageDto) {
        ingestionService.ingestEnergyUsage(usageDto);
    }

    @GetMapping("/stats")
    public Map<String, Long> getStats() {
        long success = ingestionService.getTotalSuccess();
        long failed = ingestionService.getTotalFailed();
        return Map.of(
                "totalSent", ingestionService.getTotalSent(),
                "successCount", success,
                "failedCount", failed,
                "successRate%", success + failed == 0 ? 100L : (success * 100) / (success + failed)
        );
    }
}
