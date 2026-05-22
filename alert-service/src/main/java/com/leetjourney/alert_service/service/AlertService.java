package com.leetjourney.alert_service.service;

import com.leetjourney.kafka.event.AlertingEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AlertService {

    private final EmailService emailService;

    public AlertService(EmailService emailService) {
        this.emailService = emailService;
    }

    @KafkaListener(topics = "energy-alerts", groupId = "alert-service")
    public void energyUsageAlertEvent(AlertingEvent alertingEvent) {

        log.info("Received alert event: {}", alertingEvent);

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