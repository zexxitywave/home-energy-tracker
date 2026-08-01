package com.leetjourney.ingestion_service.service;

import com.leetjourney.ingestion_service.dto.EnergyUsageDto;
import com.leetjourney.kafka.event.EnergyUsageEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
public class IngestionService {

    private final KafkaTemplate<String, EnergyUsageEvent> kafkaTemplate;
    private final Counter successCounter;
    private final Counter failureCounter;
    private final AtomicLong totalSent = new AtomicLong(0);

    public IngestionService(KafkaTemplate<String, EnergyUsageEvent> kafkaTemplate,
                            MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.successCounter = Counter.builder("ingestion.messages.success")
                .description("Total messages successfully sent to Kafka")
                .register(meterRegistry);
        this.failureCounter = Counter.builder("ingestion.messages.failed")
                .description("Total messages failed to send to Kafka")
                .register(meterRegistry);
    }

    public void ingestEnergyUsage(EnergyUsageDto input) {
        // Convert DTO to Event
        EnergyUsageEvent event = EnergyUsageEvent.builder()
                .deviceId(input.deviceId())
                .energyConsumed(input.energyConsumed())
                .timestamp(input.timestamp())
                .build();

        // Send to Kafka Topic with success/failure tracking
        kafkaTemplate.send("energy-usage", event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        long count = totalSent.incrementAndGet();
                        successCounter.increment();
                        // Log every 1000 messages to avoid log spam
                        if (count % 1000 == 0) {
                            log.info("Ingestion stats — total sent: {}, success: {}, failed: {}",
                                    count,
                                    (long) successCounter.count(),
                                    (long) failureCounter.count());
                        }
                    } else {
                        failureCounter.increment();
                        log.error("Failed to send event to Kafka: deviceId={}, error={}",
                                event.deviceId(), ex.getMessage());
                    }
                });
    }

    public long getTotalSent() {
        return totalSent.get();
    }

    public long getTotalSuccess() {
        return (long) successCounter.count();
    }

    public long getTotalFailed() {
        return (long) failureCounter.count();
    }
}
