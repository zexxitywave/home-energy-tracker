package com.leetjourney.usage_service.service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.leetjourney.kafka.event.AlertingEvent;
import com.leetjourney.kafka.event.EnergyUsageEvent;
import com.leetjourney.usage_service.client.DeviceClient;
import com.leetjourney.usage_service.client.UserClient;
import com.leetjourney.usage_service.dto.DeviceDto;
import com.leetjourney.usage_service.dto.UsageDto;
import com.leetjourney.usage_service.dto.UserDto;
import com.leetjourney.usage_service.model.Device;
import com.leetjourney.usage_service.model.DeviceEnergy;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class UsageService {

    private final InfluxDBClient influxDBClient;
    private final DeviceClient deviceClient;
    private final UserClient userClient;
    private final KafkaTemplate<String, AlertingEvent> kafkaTemplate;

    // Micrometer counters
    private final Counter usageEventsConsumedCounter;
    private final Counter usageInfluxWriteCounter;
    private final Counter alertsProducedCounter;
    private final Counter alertsProducedWarningCounter;
    private final Counter alertsProducedCriticalCounter;

    @Value("${influx.bucket}")
    private String influxBucket;

    @Value("${influx.org}")
    private String influxOrg;

    @Value("${electricity.rate.per.kwh}")
    private double electricityRate;

    @Value("${monthly.billing.days}")
    private int billingDays;

    public UsageService(
            InfluxDBClient influxDBClient,
            DeviceClient deviceClient,
            UserClient userClient,
            KafkaTemplate<String, AlertingEvent> kafkaTemplate,
            MeterRegistry meterRegistry
    ) {
        this.influxDBClient = influxDBClient;
        this.deviceClient = deviceClient;
        this.userClient = userClient;
        this.kafkaTemplate = kafkaTemplate;
        this.usageEventsConsumedCounter = Counter.builder("usage.events.consumed")
                .description("Total energy usage events consumed from Kafka")
                .register(meterRegistry);
        this.usageInfluxWriteCounter = Counter.builder("usage.influx.writes")
                .description("Total points written to InfluxDB")
                .register(meterRegistry);
        this.alertsProducedCounter = Counter.builder("usage.alerts.produced")
                .description("Total alerts produced to energy-alerts topic")
                .register(meterRegistry);
        this.alertsProducedWarningCounter = Counter.builder("usage.alerts.warning")
                .description("Total WARNING alerts produced")
                .register(meterRegistry);
        this.alertsProducedCriticalCounter = Counter.builder("usage.alerts.critical")
                .description("Total CRITICAL alerts produced")
                .register(meterRegistry);
    }

    @PostConstruct
    public void init() {
        // Batch: flush every 1 second OR when 1000 points accumulate — whichever comes first
        // This means consumer thread returns instantly; InfluxDB writes happen in background
        this.writeApi = influxDBClient.makeWriteApi(
                WriteOptions.builder()
                        .batchSize(1000)
                        .flushInterval(1000)   // ms
                        .bufferLimit(50000)    // max queued points before dropping
                        .build()
        );
        log.info("InfluxDB non-blocking write API initialized (batch=1000, flush=1s)");
    }

    @PreDestroy
    public void shutdown() {
        if (writeApi != null) {
            writeApi.flush();
            writeApi.close();
            log.info("InfluxDB write API flushed and closed");
        }
    }

    @KafkaListener(topics = "energy-usage", groupId = "usage-service")
    public void energyUsageEvent(EnergyUsageEvent energyUsageEvent, Acknowledgment ack) {
        Point point = Point.measurement("energy_usage")
                .addTag("deviceId", String.valueOf(energyUsageEvent.deviceId()))
                .addField("energyConsumed", energyUsageEvent.energyConsumed())
                .time(Instant.now(), WritePrecision.MS);

        try {
            // Synchronous write — waits for InfluxDB confirmation before acking
            influxDBClient.getWriteApiBlocking().writePoint(influxBucket, influxOrg, point);

            // Only acknowledge AFTER successful InfluxDB write
            // If InfluxDB fails → no ack → Kafka keeps message → retried on next poll
            ack.acknowledge();

            usageEventsConsumedCounter.increment();
            usageInfluxWriteCounter.increment();

        } catch (Exception e) {
            // No ack — Kafka will redeliver this message
            log.error("InfluxDB write failed for deviceId={}, will retry: {}",
                    energyUsageEvent.deviceId(), e.getMessage());
        }
    }

    @Scheduled(fixedRate = 5000)
    public void aggregateDeviceEnergyUsage() {

        Instant now = Instant.now();
        Instant oneHourAgo = now.minusSeconds(3600);

        String fluxQuery = String.format("""
        from(bucket: "%s")
          |> range(start: time(v: "%s"), stop: time(v: "%s"))
          |> filter(fn: (r) => r["_measurement"] == "energy_usage")
          |> filter(fn: (r) => r["_field"] == "energyConsumed")
          |> group(columns: ["deviceId"])
          |> sum(column: "_value")
        """, influxBucket, oneHourAgo, now);

        QueryApi queryApi = influxDBClient.getQueryApi();
        List<FluxTable> tables = queryApi.query(fluxQuery, influxOrg);

        List<DeviceEnergy> deviceEnergies = new ArrayList<>();

        for (FluxTable table : tables) {
            for (FluxRecord record : table.getRecords()) {

                String deviceIdStr = (String) record.getValueByKey("deviceId");

                Double energyConsumed = record.getValueByKey("_value") instanceof Number
                        ? ((Number) record.getValueByKey("_value")).doubleValue()
                        : 0.0;

                try {
                    DeviceDto deviceResponse = deviceClient.getDeviceById(Long.valueOf(deviceIdStr));

                    if (deviceResponse == null || deviceResponse.id() == null) {
                        continue;
                    }

                    deviceEnergies.add(
                            DeviceEnergy.builder()
                                    .deviceId(Long.valueOf(deviceIdStr))
                                    .energyConsumed(energyConsumed)
                                    .userId(deviceResponse.userId())
                                    .build()
                    );

                } catch (Exception e) {
                    log.warn("Failed to fetch device {}", deviceIdStr);
                }
            }
        }

        log.info("Aggregated device energies: {}", deviceEnergies);

        Map<Long, List<DeviceEnergy>> userDeviceEnergyMap =
                deviceEnergies.stream()
                        .filter(de -> de.getUserId() != null)
                        .collect(Collectors.groupingBy(DeviceEnergy::getUserId));

        List<Long> userIds = new ArrayList<>(userDeviceEnergyMap.keySet());

        Map<Long, Double> userThresholdMap = new HashMap<>();
        Map<Long, String> userEmailMap = new HashMap<>();

        for (Long userId : userIds) {
            try {
                UserDto user = userClient.getUserById(userId);

                if (user == null || user.id() == null || !user.alerting()) {
                    continue;
                }

                userThresholdMap.put(userId, user.energyAlertingThreshold());
                userEmailMap.put(userId, user.email());

            } catch (Exception e) {
                log.warn("Failed to fetch user {}", userId);
            }
        }

        for (Long userId : userThresholdMap.keySet()) {

            Double threshold = userThresholdMap.get(userId);
            List<DeviceEnergy> devices = userDeviceEnergyMap.get(userId);

            Double totalConsumption = devices.stream()
                    .mapToDouble(DeviceEnergy::getEnergyConsumed)
                    .sum();

            if (totalConsumption > threshold) {

                double totalKwh = totalConsumption / 1000.0;
                double estimatedCost = totalKwh * electricityRate;
                double projectedMonthlyCost = estimatedCost * billingDays;

                String alertLevel = totalConsumption > (threshold * 1.5)
                        ? "CRITICAL"
                        : "WARNING";

                String deviceNames = devices.stream()
                        .map(device -> "Device-" + device.getDeviceId())
                        .collect(Collectors.joining(", "));

                AlertingEvent alertingEvent = AlertingEvent.builder()
                        .userId(userId)
                        .message("Energy consumption threshold exceeded")
                        .threshold(threshold)
                        .energyConsumed(totalConsumption)
                        .email(userEmailMap.get(userId))
                        .totalKwh(totalKwh)
                        .estimatedCost(estimatedCost)
                        .projectedMonthlyCost(projectedMonthlyCost)
                        .alertLevel(alertLevel)
                        .deviceName(deviceNames)
                        .build();

                kafkaTemplate.send("energy-alerts", alertingEvent);

                alertsProducedCounter.increment();
                if ("CRITICAL".equals(alertLevel)) {
                    alertsProducedCriticalCounter.increment();
                } else {
                    alertsProducedWarningCounter.increment();
                }

                log.info("[{}] Alert sent for user {} — consumed: {}W, threshold: {}W, cost: ₹{}",
                        alertLevel, userId, totalConsumption, threshold, String.format("%.2f", estimatedCost));

            } else {
                log.info("User {} within threshold", userId);
            }
        }
    }
    public UsageDto getXDaysUsageForUser(Long userId, int days) {

        List<DeviceDto> devicesDto = deviceClient.getAllDevicesForUser(userId);

        List<Device> devices = new ArrayList<>();

        for (DeviceDto deviceDto : devicesDto) {
            devices.add(Device.builder()
                    .id(deviceDto.id())
                    .name(deviceDto.name())
                    .type(deviceDto.type())
                    .location(deviceDto.location())
                    .userId(deviceDto.userId())
                    .build());
        }

        if (devices.isEmpty()) {
            return UsageDto.builder()
                    .userId(userId)
                    .devices(null)
                    .build();
        }

        List<String> deviceIdStrings = devices.stream()
                .map(Device::getId)
                .filter(Objects::nonNull)
                .map(String::valueOf)
                .toList();

        Instant now = Instant.now();
        Instant start = now.minusSeconds((long) days * 24 * 3600);

        String deviceFilter = deviceIdStrings.stream()
                .map(id -> String.format("r[\"deviceId\"] == \"%s\"", id))
                .collect(Collectors.joining(" or "));

        String fluxQuery = String.format("""
            from(bucket: "%s")
              |> range(start: time(v: "%s"), stop: time(v: "%s"))
              |> filter(fn: (r) => r["_measurement"] == "energy_usage")
              |> filter(fn: (r) => r["_field"] == "energyConsumed")
              |> filter(fn: (r) => %s)
              |> group(columns: ["deviceId"])
              |> sum(column: "_value")
            """, influxBucket, start, now, deviceFilter);

        Map<Long, Double> aggregatedMap = new HashMap<>();

        try {
            QueryApi queryApi = influxDBClient.getQueryApi();
            List<FluxTable> tables = queryApi.query(fluxQuery, influxOrg);

            for (FluxTable table : tables) {
                for (FluxRecord record : table.getRecords()) {

                    String deviceIdStr = record.getValueByKey("deviceId").toString();

                    Double energyConsumed = record.getValueByKey("_value") instanceof Number
                            ? ((Number) record.getValueByKey("_value")).doubleValue()
                            : 0.0;

                    Long deviceId = Long.valueOf(deviceIdStr);

                    aggregatedMap.put(
                            deviceId,
                            aggregatedMap.getOrDefault(deviceId, 0.0) + energyConsumed
                    );
                }
            }

        } catch (Exception e) {
            log.error("Influx query failed: {}", e.getMessage());
        }

        for (Device device : devices) {
            device.setEnergyConsumed(
                    aggregatedMap.getOrDefault(device.getId(), 0.0)
            );
        }

        List<DeviceDto> resultDevices = devices.stream()
                .map(d -> DeviceDto.builder()
                        .id(d.getId())
                        .name(d.getName())
                        .type(d.getType())
                        .location(d.getLocation())
                        .userId(d.getUserId())
                        .energyConsumed(d.getEnergyConsumed())
                        .build())
                .toList();

        return UsageDto.builder()
                .userId(userId)
                .devices(resultDevices)
                .build();
    }
}