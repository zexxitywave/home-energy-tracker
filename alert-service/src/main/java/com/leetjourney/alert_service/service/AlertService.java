package com.leetjourney.alert_service.service;

import com.leetjourney.kafka.event.AlertingEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AlertService {

    private final EmailService emailService;
    private final Counter alertsConsumedCounter;
    private final Counter alertsWarningCounter;
    private final Counter alertsCriticalCounter;

    public AlertService(EmailService emailService, MeterRegistry meterRegistry) {
        this.emailService = emailService;
        this.alertsConsumedCounter = Counter.builder("alert.messages.consumed")
                .description("Total alert messages consumed from Kafka")
                .register(meterRegistry);
        this.alertsWarningCounter = Counter.builder("alert.messages.warning")
                .description("Total WARNING level alerts consumed")
                .register(meterRegistry);
        this.alertsCriticalCounter = Counter.builder("alert.messages.critical")
                .description("Total CRITICAL level alerts consumed")
                .register(meterRegistry);
    }

    @KafkaListener(topics = "energy-alerts", groupId = "alert-service")
    public void energyUsageAlertEvent(AlertingEvent alertingEvent) {

        log.info("Received alert event: {}", alertingEvent);

        alertsConsumedCounter.increment();

        if ("CRITICAL".equalsIgnoreCase(alertingEvent.getAlertLevel())) {
            alertsCriticalCounter.increment();
        } else {
            alertsWarningCounter.increment();
        }

        String subject = "Energy Usage Alert for User " + alertingEvent.getUserId();

        String message = """
                Alert Level: %s
                
                Alert Message: %s
                
                Threshold: %.2f W
                
                Energy Consumed: %.2f W
                
                Energy Used: %.2f kWh
                
                Estimated Cost: ₹%.2f
                
                Projected Monthly Bill: ₹%.2f
                
                Devices: %s
                """.formatted(
                alertingEvent.getAlertLevel(),
                alertingEvent.getMessage(),
                alertingEvent.getThreshold(),
                alertingEvent.getEnergyConsumed(),
                alertingEvent.getTotalKwh(),
                alertingEvent.getEstimatedCost(),
                alertingEvent.getProjectedMonthlyCost(),
                alertingEvent.getDeviceName()
        );

        emailService.sendEmail(
                alertingEvent.getEmail(),
                subject,
                message,
                alertingEvent.getUserId()
        );
    }
}