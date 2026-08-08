# Home Energy Tracker — System Design

> High-Level Design (HLD) and Low-Level Design (LLD)
> Stack: Java 21 · Spring Boot 4 · Apache Kafka (KRaft) · InfluxDB 2.x · PostgreSQL · AWS S3 · Spring AI + Ollama · Docker

---

## Table of Contents

1. [High-Level Design (HLD)](#1-high-level-design-hld)
   - [1.1 System Overview](#11-system-overview)
   - [1.2 Full Architecture Diagram](#12-full-architecture-diagram)
   - [1.3 Data Flow — Ingestion to Alert](#13-data-flow--ingestion-to-alert)
   - [1.4 Data Flow — Complaint Upload](#14-data-flow--complaint-upload)
   - [1.5 Data Flow — AI Insight](#15-data-flow--ai-insight)
   - [1.6 Infrastructure Layer](#16-infrastructure-layer)
   - [1.7 API Gateway — Routing and Security](#17-api-gateway--routing-and-security)
   - [1.8 Observability Layer](#18-observability-layer)
2. [Low-Level Design (LLD)](#2-low-level-design-lld)
   - [2.1 ingestion-service](#21-ingestion-service)
   - [2.2 usage-service](#22-usage-service)
   - [2.3 alert-service](#23-alert-service)
   - [2.4 complaint-service](#24-complaint-service)
   - [2.5 user-service](#25-user-service)
   - [2.6 device-service](#26-device-service)
   - [2.7 insight-service](#27-insight-service)
   - [2.8 api-gateway](#28-api-gateway)
   - [2.9 Kafka Topics Design](#29-kafka-topics-design)
   - [2.10 Database Schemas](#210-database-schemas)
   - [2.11 InfluxDB Schema](#211-influxdb-schema)
   - [2.12 AWS S3 Key Structure](#212-aws-s3-key-structure)
   - [2.13 Sequence Diagrams](#213-sequence-diagrams)

---

## 1. High-Level Design (HLD)

### 1.1 System Overview

Home Energy Tracker is a real-time IoT energy monitoring platform built as a microservices system. Smart devices send energy readings to an ingestion endpoint. Readings flow through a Kafka pipeline where they are persisted to a time-series database, aggregated, and compared against per-user thresholds. When a threshold is breached, the system classifies the alert as WARNING or CRITICAL and delivers a billing projection via email. Users can also file complaints with file attachments (stored in AWS S3) and request AI-generated energy saving tips powered by a local LLM.

```
┌─────────────────────────────────────────────────────────────────┐
│                     CLIENTS / POSTMAN / SIMULATOR               │
└──────────────────────────────┬──────────────────────────────────┘
                               │ HTTP
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                   API GATEWAY  (port 9000)                      │
│     Spring Cloud Gateway MVC · Resilience4j Circuit Breakers    │
│          OAuth2 Resource Server · Keycloak JWT (optional)       │
└──┬──────────┬────────────┬──────────┬───────────┬──────────┬───┘
   │          │            │          │           │          │
   ▼          ▼            ▼          ▼           ▼          ▼
user      device      ingestion   usage       alert      insight
:8080     :8081        :8082      :8083       :8084       :8085
                                                      complaint
                                                        :8086
```

---

### 1.2 Full Architecture Diagram

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                         HOME ENERGY TRACKER                                  ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  ┌─────────┐   HTTP POST    ┌─────────────────────────────────────────────┐ ║
║  │Simulator│ ─────────────► │           API GATEWAY :9000                 │ ║
║  │10 thread│                │  Route /api/v1/user/**    → user-svc :8080  │ ║
║  │1000 r/s │                │  Route /api/v1/device/**  → device-svc:8081 │ ║
║  └─────────┘                │  Route /api/v1/ingestion/**→ ingest-svc:8082│ ║
║                             │  Route /api/v1/usage/**   → usage-svc :8083 │ ║
║  ┌─────────┐   HTTP POST    │  Route /api/v1/alert/**   → alert-svc :8084 │ ║
║  │ Postman │ ─────────────► │  Route /api/v1/insight/** → insight-svc:8085│ ║
║  └─────────┘                │  Circuit Breaker (Resilience4j) per route   │ ║
║                             └──────────────┬────────────────────────────── ┘ ║
║                                            │                                 ║
║              ┌─────────────────────────────┼─────────────────────────┐      ║
║              │                             │                          │      ║
║              ▼                             ▼                          ▼      ║
║  ┌─────────────────────┐   ┌─────────────────────┐   ┌────────────────────┐ ║
║  │   user-service      │   │   device-service     │   │ ingestion-service  │ ║
║  │   :8080             │   │   :8081              │   │ :8082              │ ║
║  │                     │   │                      │   │                    │ ║
║  │ UserController      │   │ DeviceController     │   │ IngestionController│ ║
║  │  POST /user         │   │  POST /device/create │   │  POST /ingestion   │ ║
║  │  GET  /user/{id}    │   │  GET  /device/{id}   │   │  GET  /stats       │ ║
║  │  PUT  /user/{id}    │   │  PUT  /device/{id}   │   │                    │ ║
║  │  DEL  /user/{id}    │   │  DEL  /device/{id}   │   │ ParallelSimulator  │ ║
║  │                     │   │  GET  /device/user/  │   │  10 threads        │ ║
║  │ PostgreSQL          │   │      {userId}        │   │  1000 req/interval │ ║
║  │  table: users       │   │                      │   │                    │ ║
║  │                     │   │ PostgreSQL           │   │ KafkaTemplate      │ ║
║  └─────────────────────┘   │  table: device       │   │  → energy-usage    │ ║
║                             └──────────────────────┘   │  (5 partitions)    │ ║
║                                                        └────────┬───────────┘ ║
║                                                                 │ Kafka Produce║
║                                                                 ▼             ║
║                        ┌─────────────────────────────────────────────────┐   ║
║                        │        Apache Kafka (KRaft, no ZooKeeper)       │   ║
║                        │                                                  │   ║
║                        │   Topic: energy-usage   (5 partitions)          │   ║
║                        │   ├─ partition 0: ~144K msgs                    │   ║
║                        │   ├─ partition 1: ~150K msgs                    │   ║
║                        │   ├─ partition 2: ~157K msgs  RoundRobin        │   ║
║                        │   ├─ partition 3: ~173K msgs  partitioner       │   ║
║                        │   └─ partition 4: ~138K msgs                    │   ║
║                        │                                                  │   ║
║                        │   Topic: energy-alerts  (5 partitions)          │   ║
║                        │   └─ consumer group: alert-service              │   ║
║                        └───────────────┬──────────────────────────────── ┘   ║
║                                        │ Kafka Consume (manual ack)          ║
║                                        ▼                                     ║
║                        ┌─────────────────────────────────────────────────┐   ║
║                        │           usage-service :8083                   │   ║
║                        │                                                  │   ║
║                        │  @KafkaListener (concurrency=5, ack=manual)     │   ║
║                        │  energyUsageEvent(event, ack)                   │   ║
║                        │   └─► writeApiBlocking.writePoint()             │   ║
║                        │   └─► ack.acknowledge() only on success         │   ║
║                        │                                                  │   ║
║                        │  @Scheduled(fixedRate=5000)                     │   ║
║                        │  aggregateDeviceEnergyUsage()                   │   ║
║                        │   └─► Flux query: 1-hr rolling window           │   ║
║                        │   └─► group by deviceId → sum                   │   ║
║                        │   └─► HTTP GET device-service (userId lookup)   │   ║
║                        │   └─► HTTP GET user-service (threshold lookup)  │   ║
║                        │   └─► totalConsumption > threshold?             │   ║
║                        │       ├─ > threshold×1.5 → CRITICAL             │   ║
║                        │       └─ > threshold     → WARNING              │   ║
║                        │   └─► cooldown check (1hr per user)             │   ║
║                        │   └─► produce AlertingEvent → energy-alerts     │   ║
║                        └───────────┬────────────────────┬────────────────┘   ║
║                                    │                     │                   ║
║                                    ▼                     ▼                   ║
║                        ┌─────────────────┐   ┌─────────────────────────┐    ║
║                        │   InfluxDB      │   │   energy-alerts topic   │    ║
║                        │   :8072         │   │   (5 partitions)        │    ║
║                        │                 │   └──────────┬──────────────┘    ║
║                        │ bucket:         │              │ Kafka Consume      ║
║                        │  usage-bucket   │              ▼                   ║
║                        │ measurement:    │   ┌─────────────────────────┐    ║
║                        │  energy_usage   │   │   alert-service :8084   │    ║
║                        │ tag: deviceId   │   │                         │    ║
║                        │ field:          │   │ @KafkaListener          │    ║
║                        │  energyConsumed │   │  (concurrency=5)        │    ║
║                        │ retention: 1w   │   │                         │    ║
║                        │                 │   │ @Async EmailService     │    ║
║                        └─────────────────┘   │  SMTP → Resend          │    ║
║                                              │  (smtp.resend.com:465)  │    ║
║                                              │                         │    ║
║                                              │ AlertRepository         │    ║
║                                              │  → PostgreSQL           │    ║
║                                              │  table: alert           │    ║
║                                              └─────────────────────────┘    ║
║                                                                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  COMPLAINT & AI SERVICES (independent of Kafka pipeline)                     ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  ┌───────────────────────────────────────┐                                  ║
║  │        complaint-service :8086        │                                  ║
║  │                                       │                                  ║
║  │  POST /api/complaints (multipart)     │                                  ║
║  │   └─► save Complaint → PostgreSQL     │                                  ║
║  │   └─► CompletableFuture.runAsync()    │                                  ║
║  │       └─► parallelStream()            │                                  ║
║  │           └─► S3Client.putObject()    │──────► AWS S3 (ap-south-1)      ║
║  │               key: users/{uid}/       │        bucket:                   ║
║  │                    complaints/{cid}/  │        home-energy-tracker-      ║
║  │                    {uuid}-{filename}  │        complaints                ║
║  │                                       │                                  ║
║  │  GET  /api/complaints/{id}            │                                  ║
║  │  GET  /api/complaints/user/{userId}   │                                  ║
║  │  PUT  /api/complaints/{id}/status     │                                  ║
║  │  DELETE /api/complaints/{id}          │                                  ║
║  └───────────────────────────────────────┘                                  ║
║                                                                              ║
║  ┌───────────────────────────────────────┐                                  ║
║  │        insight-service :8085          │                                  ║
║  │  (Spring Boot 3.5 + Spring AI)        │                                  ║
║  │                                       │                                  ║
║  │  GET /api/v1/insight/saving-tips/{id} │                                  ║
║  │  GET /api/v1/insight/overview/{id}    │                                  ║
║  │   └─► HTTP GET usage-service          │                                  ║
║  │       /api/v1/usage/{id}?days=3       │                                  ║
║  │   └─► Build prompt with usage data    │                                  ║
║  │   └─► OllamaChatModel.call(prompt)    │──────► Ollama :11434             ║
║  │   └─► return InsightDto               │        model: qwen2.5:3b         ║
║  └───────────────────────────────────────┘                                  ║
║                                                                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  OBSERVABILITY                                                               ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  Each service exposes /actuator/prometheus                                   ║
║                                                                              ║
║  Prometheus :9091 ──scrape every 15s──► all services                        ║
║  Grafana :3000 ──────query PromQL──────► Prometheus                         ║
║                                                                              ║
║  Dashboards:                                                                 ║
║   • Overview (19 panels)                                                    ║
║   • Service Health (16 panels, per-service dropdown)                        ║
║   • Kafka & Ingestion Pipeline (19 panels)                                  ║
║                                                                              ║
║  Alert Rules: 19 rules / 6 groups                                           ║
║   ServiceDown · HighHeapUsage · CriticalHeapUsage · FrequentGCPauses        ║
║   HighBlockedThreads · IngestionStalled · HikariCP exhaustion               ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

---

### 1.3 Data Flow — Ingestion to Alert

```
  ParallelDataSimulator
  (10 threads, 1000 req/interval)
         │
         │  POST /api/v1/ingestion
         │  Body: { deviceId, energyConsumed, timestamp }
         ▼
  IngestionController
         │
         ▼
  IngestionService.ingestEnergyUsage()
         │  EnergyUsageEvent { deviceId, energyConsumed, timestamp }
         │  kafkaTemplate.send("energy-usage", event)
         │  whenComplete → successCounter++ / failureCounter++
         ▼
  Kafka Topic: energy-usage
  (5 partitions, RoundRobin)
         │
         │  @KafkaListener
         │  concurrency=5, ack-mode=manual
         ▼
  UsageService.energyUsageEvent(event, ack)
         │
         ├── writeApiBlocking.writePoint()
         │        │
         │        ▼
         │   InfluxDB: energy_usage measurement
         │   tag: deviceId | field: energyConsumed | time: now(ms)
         │        │
         │   SUCCESS → ack.acknowledge() → Kafka commits offset
         │   FAILURE → no ack → Kafka redelivers (zero data loss)
         │
         └── @Scheduled(fixedRate=5000ms)
             aggregateDeviceEnergyUsage()
                  │
                  ├── Flux query: last 1 hour
                  │   from(bucket) |> range(-1h) |> group(deviceId) |> sum()
                  │
                  ├── HTTP GET device-service /api/v1/device/{deviceId}
                  │   → resolve userId
                  │
                  ├── HTTP GET user-service /api/v1/user/{userId}
                  │   → get threshold, email, alerting flag
                  │
                  ├── totalConsumption = sum(all devices for user)
                  │
                  ├── cooldown check: lastAlertTime[userId] < now - 1hr?
                  │
                  └── if totalConsumption > threshold:
                       ├── > threshold × 1.5 → alertLevel = "CRITICAL"
                       └── > threshold       → alertLevel = "WARNING"
                           │
                           │  AlertingEvent {
                           │    userId, email, threshold, energyConsumed,
                           │    totalKwh, estimatedCost (₹/kWh × 8.5),
                           │    projectedMonthlyCost (× 30 days),
                           │    alertLevel, deviceName
                           │  }
                           ▼
                      Kafka Topic: energy-alerts (5 partitions)
                           │
                           │  @KafkaListener concurrency=5
                           ▼
                      AlertService.energyUsageAlertEvent(alertingEvent)
                           │
                           ├── alertsConsumedCounter++
                           ├── alertsWarning/CriticalCounter++
                           │
                           └── EmailService.sendEmail() [@Async]
                                    │
                                    ├── JavaMailSender (Resend SMTP :465)
                                    │   to: user email
                                    │   subject: "Energy Usage Alert for User {id}"
                                    │
                                    └── AlertRepository.save()
                                         → PostgreSQL: table alert
                                           { id, userId, createdAt, sent }
```

---

### 1.4 Data Flow — Complaint Upload

```
  Client
    │
    │  POST /api/complaints   (multipart/form-data)
    │  Part "complaint": JSON string
    │  Part "files":     List<MultipartFile>  (up to 100MB total, 50MB/file)
    ▼
  ComplaintController.createComplaint()
    │
    ├── ObjectMapper.readValue(complaintJson, Complaint.class)
    │
    └── ComplaintServiceImpl.createComplaint(complaint, files)
              │
              ├── complaint.setStatus(OPEN)
              │
              ├── complaintRepository.save(complaint)
              │   → PostgreSQL: table complaints
              │     { id, userId, title, description, category,
              │       status, priority, imageKeys, adminResponse,
              │       createdAt, updatedAt }
              │   ← returns savedComplaint (with auto-generated id)
              │
              ├── RETURN 200 OK to client ← (fast response, async below)
              │
              └── CompletableFuture.runAsync() ← background thread
                        │
                        └── files.parallelStream()
                                 │  for each file simultaneously:
                                 ▼
                              S3ServiceImpl.uploadFile()
                                 │
                                 ├── key = "users/{userId}/complaints/
                                 │          {complaintId}/{uuid}-{filename}"
                                 │
                                 ├── PutObjectRequest.builder()
                                 │     .bucket("home-energy-tracker-complaints")
                                 │     .key(fileName)
                                 │     .contentType(contentType)
                                 │     .metadata({ uploaded-by, module,
                                 │                 user-id, complaint-id })
                                 │
                                 └── s3Client.putObject() → AWS S3 (ap-south-1)
                                      │
                                      ▼
                              returns S3 key string
                                 │
                                 ▼
                        imageKeys = join(allKeys, ",")
                        complaintRepository.save(complaint with imageKeys)
```

---

### 1.5 Data Flow — AI Insight

```
  Client
    │
    │  GET /api/v1/insight/saving-tips/{userId}
    │  GET /api/v1/insight/overview/{userId}
    ▼
  InsightController
    │
    └── InsightService.getSavingsTips(userId)
    │         │
    │         ├── HTTP GET usage-service
    │         │   /api/v1/usage/{userId}?days=3
    │         │   → UsageDto { userId, List<DeviceDto> }
    │         │     each DeviceDto: { id, name, type, location,
    │         │                       userId, energyConsumed (3-day sum) }
    │         │
    │         ├── totalUsage = sum(device.energyConsumed)
    │         │
    │         ├── prompt = "Total energy used: {totalUsage}W.
    │         │            How can I reduce consumption?
    │         │            How does it compare to avg households?"
    │         │
    │         └── OllamaChatModel.call(prompt)
    │                   │
    │                   └── HTTP POST localhost:11434
    │                       model: qwen2.5:3b
    │                       ← AI-generated text response
    │
    └── return InsightDto { userId, tips (AI text), energyUsage }
```

---

### 1.6 Infrastructure Layer

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Docker Compose Stack                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  MESSAGING                                                    │  │
│  │                                                               │  │
│  │  kafka          apache/kafka:latest                          │  │
│  │                 internal: kafka:9092 (PLAINTEXT)             │  │
│  │                 external: localhost:9094 (EXTERNAL)          │  │
│  │                 controller: kafka:9093                       │  │
│  │                 KRaft mode (no ZooKeeper)                    │  │
│  │                 KAFKA_NUM_PARTITIONS=5 (auto-create)         │  │
│  │                 volume: ./docker/kafka_data                  │  │
│  │                                                               │  │
│  │  kafka-ui       kafbat/kafka-ui:latest  :8070→8080           │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  STORAGE                                                      │  │
│  │                                                               │  │
│  │  influxdb       influxdb:2.7           :8072→8086            │  │
│  │                 org: leetjourney                             │  │
│  │                 bucket: usage-bucket                         │  │
│  │                 token: my-token                              │  │
│  │                 retention: 1w                                │  │
│  │                 volume: ./influxdb_data                      │  │
│  │                                                               │  │
│  │  postgres       postgres:16 (Windows local service)          │  │
│  │                 port: 5433                                   │  │
│  │                 db: home_energy_tracker                      │  │
│  │                 user: postgres / postgres123                 │  │
│  │                                                               │  │
│  │  AWS S3 (external)  ap-south-1 (Mumbai)                     │  │
│  │                 bucket: home-energy-tracker-complaints        │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  OBSERVABILITY                                                │  │
│  │                                                               │  │
│  │  prometheus     prom/prometheus:v3.1.0  :9091→9090           │  │
│  │                 scrape_interval: 15s                         │  │
│  │                 retention: 5d                                │  │
│  │                 config: ./docker/prometheus/prometheus.yml   │  │
│  │                 rules: ./docker/prometheus/alert-rules.yml   │  │
│  │                 extra_hosts: host.docker.internal→gateway    │  │
│  │                                                               │  │
│  │  grafana        grafana:11.4.0          :3000→3000           │  │
│  │                 admin/admin                                  │  │
│  │                 provisioning: ./docker/grafana/provisioning  │  │
│  │                 3 dashboards auto-loaded                     │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  EMAIL (dev)                                                  │  │
│  │                                                               │  │
│  │  mailpit        axllent/mailpit:latest                       │  │
│  │                 SMTP: :1025   Web UI: :8025                  │  │
│  │  (prod: Resend SMTP smtp.resend.com:465)                     │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

### 1.7 API Gateway — Routing and Security

```
  Client Request → API Gateway :9000
         │
         ├── SecurityConfig: all /api/v1/** → permitAll (JWT optional)
         │   Keycloak OAuth2 Resource Server configured but optional
         │   jwk-set-uri: keycloak:8080/realms/het-security-realm
         │
         ├── Route matching:
         │
         │   /api/v1/user/**        → http://localhost:8080
         │   /api/v1/device/**      → http://localhost:8081
         │   /api/v1/ingestion/**   → http://localhost:8082
         │   /api/v1/usage/**       → http://localhost:8083
         │   /api/v1/alert/**       → http://localhost:8084
         │   /api/v1/insight/**     → http://localhost:8085
         │
         │   /docs/{svc}/v3/api-docs → rewrite to /v3/api-docs on svc
         │
         └── Circuit Breaker (Resilience4j) per route:
             slidingWindow: COUNT_BASED, size: 8
             failureRateThreshold: 20%
             waitDurationInOpenState: 5s
             permittedCallsInHalfOpen: 2
             autoTransitionToHalfOpen: true
             onOpen: forward to /fallbackRoute → 503 SERVICE_UNAVAILABLE
```

---

### 1.8 Observability Layer

```
  ┌──────────────────────────────────────────────────────────────┐
  │             OBSERVABILITY STACK                              │
  │                                                              │
  │  Spring Boot Services                                        │
  │  (each exposes /actuator/prometheus)                         │
  │  Micrometer metrics:                                         │
  │   • jvm_memory_used/max_bytes{area=heap/nonheap}            │
  │   • jvm_gc_pause_seconds_{sum,count,bucket}                 │
  │   • jvm_threads_{live,daemon,states}_threads                │
  │   • http_server_requests_seconds_{count,sum,bucket}         │
  │   • hikaricp_connections_{active,idle,pending}              │
  │   • hikaricp_connections_{acquire,usage}_seconds_bucket     │
  │   • ingestion_messages_{success,failed}_total               │
  │   • usage_events_consumed_total                             │
  │   • usage_influx_writes_total                               │
  │   • usage_alerts_{produced,warning,critical}_total          │
  │   • alert_messages_{consumed,warning,critical}_total        │
  │   • process_cpu_usage, process_uptime_seconds               │
  │                                                              │
  │  tag: application=${spring.application.name}                │
  │  histograms: percentiles-histogram=true (p50/p95/p99)       │
  │                                                              │
  │  Prometheus scrapes host services via:                       │
  │  host.docker.internal:{port}/actuator/prometheus            │
  │  scrape_interval: 15s  evaluation_interval: 15s             │
  │                                                              │
  │  19 Alert Rules / 6 groups:                                 │
  │  ┌─────────────────────────────────────────────────────┐   │
  │  │ service-availability: ServiceDown(30s), Restart     │   │
  │  │ http-errors: High5xx, High4xx, HighP95, SlowP99,    │   │
  │  │              ZeroRequestRate                        │   │
  │  │ jvm-memory: HeapWarn(80%), HeapCrit(92%),           │   │
  │  │             FrequentGC, LongGCPause                 │   │
  │  │ jvm-threads: HighBlocked(>10), ThreadSpike(>200)    │   │
  │  │ hikaricp: PoolExhausted, PoolCritical,              │   │
  │  │           SlowAcquisition                          │   │
  │  │ ingestion: Stalled, FailureDetected,                │   │
  │  │            HighFailureRatio(>5%)                    │   │
  │  └─────────────────────────────────────────────────────┘   │
  │                                                              │
  │  Grafana Dashboards (auto-provisioned):                     │
  │   1. Overview              uid: het-overview      19 panels │
  │   2. Service Health        uid: het-service-health 16 panels│
  │   3. Kafka Pipeline        uid: het-kafka-pipeline 19 panels│
  │                                                              │
  └──────────────────────────────────────────────────────────────┘
```

---

## 2. Low-Level Design (LLD)

### 2.1 ingestion-service

**Port:** 8082 | **No DB** | **Kafka Producer only**

```
com.leetjourney.ingestion_service
│
├── controller/
│   └── IngestionController
│       ├── POST /api/v1/ingestion        → ingestData(EnergyUsageDto)
│       └── GET  /api/v1/ingestion/stats  → getStats()
│           returns: { totalSent, successCount, failedCount, successRate% }
│
├── service/
│   └── IngestionService
│       ├── KafkaTemplate<String, EnergyUsageEvent>
│       ├── Counter: ingestion.messages.success (Micrometer)
│       ├── Counter: ingestion.messages.failed  (Micrometer)
│       ├── AtomicLong: totalSent
│       │
│       └── ingestEnergyUsage(EnergyUsageDto)
│           ├── build EnergyUsageEvent { deviceId, energyConsumed, timestamp }
│           ├── kafkaTemplate.send("energy-usage", event)
│           └── whenComplete callback:
│               ├── success → totalSent++, successCounter++
│               │   log every 1000 messages
│               └── failure → failureCounter++, log error
│
├── simulation/
│   └── ParallelDataSimulator implements CommandLineRunner
│       ├── ExecutorService: newCachedThreadPool (core=10)
│       ├── @Scheduled(fixedRateString="${simulation.interval-ms}")
│       │   sendMockData()
│       │   ├── batchSize = requestsPerInterval / parallelThreads
│       │   ├── for each thread: submit Runnable
│       │   └── each Runnable: POST to /api/v1/ingestion
│       │       { deviceId: random(1-5), energyConsumed: random(0.0-4.0) }
│       └── @PreDestroy: executorService.shutdown()
│
├── dto/
│   └── EnergyUsageDto (record)
│       { Long deviceId, double energyConsumed, Instant timestamp }
│
└── kafka/event/
    └── EnergyUsageEvent (record, @Builder)
        { Long deviceId, double energyConsumed,
          @JsonFormat(STRING) Instant timestamp }

Config:
  simulation.requests-per-interval = 1000
  simulation.interval-ms           = 1000
  simulation.parallel-threads      = 10
  kafka.bootstrap-servers          = localhost:9094
  kafka.template.default-topic     = energy-usage
```

---

### 2.2 usage-service

**Port:** 8083 | **InfluxDB** | **Kafka Consumer + Producer**

```
com.leetjourney.usage_service
│
├── controller/
│   └── UsageController
│       ├── GET /api/v1/usage/{userId}?days=3
│       │   → UsageDto { userId, List<DeviceDto with energyConsumed> }
│       └── GET /api/v1/usage/report/{userId}?days=7
│           → PDF report (byte[]) via PdfReportService
│
├── service/
│   └── UsageService
│       ├── Dependencies:
│       │   ├── InfluxDBClient (OkHttp: connect=30s, read=60s, write=30s)
│       │   ├── DeviceClient  (Feign → device-service :8081)
│       │   ├── UserClient    (Feign → user-service :8080)
│       │   └── KafkaTemplate<String, AlertingEvent>
│       │
│       ├── State:
│       │   ├── ConcurrentHashMap<Long, Instant> lastAlertTime  (cooldown)
│       │   └── ALERT_COOLDOWN_SECONDS = 3600
│       │
│       ├── Micrometer Counters:
│       │   ├── usage.events.consumed
│       │   ├── usage.influx.writes
│       │   ├── usage.alerts.produced
│       │   ├── usage.alerts.warning
│       │   └── usage.alerts.critical
│       │
│       ├── @KafkaListener(topics="energy-usage", concurrency=5, ack=manual)
│       │   energyUsageEvent(EnergyUsageEvent event, Acknowledgment ack)
│       │   ├── build Point: measurement=energy_usage
│       │   │   tag: deviceId | field: energyConsumed | time: now(MS)
│       │   ├── try: influxDBClient.getWriteApiBlocking().writePoint()
│       │   │   └── success: ack.acknowledge() + counters++
│       │   └── catch: log error, NO ack → Kafka redelivers
│       │
│       ├── @Scheduled(fixedRate=5000)
│       │   aggregateDeviceEnergyUsage()
│       │   ├── Flux query (1-hr rolling window):
│       │   │   from(usage-bucket) |> range(-1hr,now)
│       │   │   |> filter(measurement=energy_usage, field=energyConsumed)
│       │   │   |> group(deviceId) |> sum()
│       │   ├── for each device: GET device-service → get userId
│       │   ├── group devices by userId
│       │   ├── for each user: GET user-service → threshold, email, alerting
│       │   ├── totalConsumption = sum(devices for user)
│       │   ├── cooldown check: skip if alerted < 1hr ago
│       │   ├── if totalConsumption > threshold:
│       │   │   ├── alertLevel: CRITICAL if >threshold×1.5 else WARNING
│       │   │   ├── totalKwh = consumption / 1000
│       │   │   ├── estimatedCost = totalKwh × 8.5 (₹/kWh)
│       │   │   ├── projectedMonthlyCost = estimatedCost × 30
│       │   │   ├── build AlertingEvent
│       │   │   ├── kafkaTemplate.send("energy-alerts", event)
│       │   │   └── lastAlertTime.put(userId, now)
│       │   └── else: log "within threshold"
│       │
│       └── getXDaysUsageForUser(userId, days)
│           ├── GET device-service: all devices for user
│           ├── Flux query: sum per deviceId for last N days
│           └── return UsageDto with per-device consumption
│
├── config/
│   └── InfluxDBConfig
│       └── @Bean InfluxDBClient
│           OkHttpClient: connect=30s, read=60s, write=30s
│           url=http://localhost:8072, token=my-token, org=leetjourney
│
└── client/
    ├── DeviceClient (Feign)
    │   GET /api/v1/device/{id}
    │   GET /api/v1/device/user/{userId}
    └── UserClient (Feign)
        GET /api/v1/user/{id}

Config:
  kafka.consumer.concurrency     = 5
  kafka.listener.ack-mode        = manual
  kafka.consumer.max.poll.records= 500
  kafka.producer.partitioner     = RoundRobinPartitioner
  electricity.rate.per.kwh       = 8.5
  monthly.billing.days           = 30
```

---

### 2.3 alert-service

**Port:** 8084 | **PostgreSQL** | **Kafka Consumer + Email**

```
com.leetjourney.alert_service
│
├── service/
│   └── AlertService  (Kafka consumer only, no HTTP endpoints)
│       ├── Dependencies:
│       │   ├── EmailService
│       │   └── MeterRegistry
│       │
│       ├── Micrometer Counters:
│       │   ├── alert.messages.consumed
│       │   ├── alert.messages.warning
│       │   └── alert.messages.critical
│       │
│       └── @KafkaListener(topics="energy-alerts", concurrency=5)
│           energyUsageAlertEvent(AlertingEvent alertingEvent)
│           ├── alertsConsumedCounter++
│           ├── if CRITICAL → criticalCounter++ else warningCounter++
│           └── emailService.sendEmail(email, subject, message, userId)
│
├── service/
│   └── EmailService
│       ├── @Async("emailTaskExecutor")  (dedicated thread pool)
│       ├── JavaMailSender (Resend SMTP: smtp.resend.com:465, SSL)
│       ├── MimeMessage: HTML email with billing details
│       ├── from: no-reply@zexxity.online
│       └── AlertRepository.save(Alert { userId, createdAt, sent=true })
│
├── config/
│   └── AsyncConfig
│       └── @Bean("emailTaskExecutor") ThreadPoolTaskExecutor
│           core=10, max=50, queue=1000
│           waitForTasksOnShutdown=true, awaitTermination=30s
│
└── entity/
    └── Alert
        { id (PK), userId, createdAt (LocalDateTime), sent (boolean) }

Config:
  kafka.consumer.concurrency     = 5
  kafka.consumer.max.poll.records= 500
  hikaricp.maximum-pool-size     = 20
  mail.host                      = smtp.resend.com
  mail.port                      = 465
  mail.smtp.ssl.enable           = true
```

---

### 2.4 complaint-service

**Port:** 8086 | **PostgreSQL + AWS S3** | **No Kafka**

```
com.todo.complaintservice
│
├── controller/
│   └── ComplaintController
│       ├── POST   /api/complaints          (multipart/form-data)
│       │   @RequestPart("complaint") String complaintJson
│       │   @RequestPart("files") List<MultipartFile>
│       ├── GET    /api/complaints/{id}
│       ├── GET    /api/complaints/user/{userId}
│       ├── GET    /api/complaints
│       ├── PUT    /api/complaints/{id}/status?status=RESOLVED
│       └── DELETE /api/complaints/{id}
│
├── service/
│   └── ComplaintServiceImpl
│       ├── createComplaint(complaint, files)
│       │   ├── complaint.setStatus(OPEN)
│       │   ├── complaintRepository.save(complaint) ← synchronous, gets id
│       │   ├── return savedComplaint (200 OK sent to client)
│       │   └── CompletableFuture.runAsync():
│       │       ├── files.stream() → List<FileUploadData>
│       │       │   { fileName, contentType, bytes[] }
│       │       └── uploadFiles.parallelStream()
│       │           → s3Service.uploadFile(name, type, bytes, userId, cid)
│       │           → collect S3 keys
│       │           → complaintRepository.save(imageKeys)
│       │
│       ├── getComplaintById(id)
│       ├── getComplaintsByUser(userId)
│       ├── getAllComplaints()
│       ├── updateComplaintStatus(id, status)
│       └── deleteComplaint(id)
│
├── service/
│   └── S3ServiceImpl
│       └── uploadFile(filename, contentType, bytes[], userId, complaintId)
│           ├── key = "users/{userId}/complaints/{complaintId}/{uuid}-{fn}"
│           ├── PutObjectRequest with metadata:
│           │   uploaded-by, module, original-file-name, user-id, complaint-id
│           ├── s3Client.putObject(request, RequestBody.fromBytes(bytes))
│           └── log: duration(ms), size(bytes), key
│
├── config/
│   └── S3Config
│       └── @Bean S3Client
│           region: ap-south-1
│           credentials: AwsBasicCredentials (from env vars)
│
└── entity/
    └── Complaint
        { id (PK), userId, title, description (2000),
          category (BILLING/TECHNICAL/SERVICE/OTHER),
          status (OPEN/IN_PROGRESS/RESOLVED/REJECTED),
          priority (LOW/MEDIUM/HIGH/CRITICAL),
          imageKeys (TEXT, comma-separated S3 keys),
          adminResponse (2000), createdAt, updatedAt }
        @PrePersist: status=OPEN, createdAt=updatedAt=now
        @PreUpdate: updatedAt=now

Config:
  multipart.max-file-size        = 50MB
  multipart.max-request-size     = 100MB
  hikaricp.maximum-pool-size     = 10
  aws.region                     = ap-south-1
  aws.s3.bucket-name             = home-energy-tracker-complaints
```

---

### 2.5 user-service

**Port:** 8080 | **PostgreSQL**

```
com.leetjourney.user_service
│
├── controller/
│   └── UserController
│       ├── POST   /api/v1/user        → 201 CREATED
│       ├── GET    /api/v1/user/{id}   → 200 / 404
│       ├── PUT    /api/v1/user/{id}   → 200
│       └── DELETE /api/v1/user/{id}  → 204
│
├── service/
│   └── UserService
│       ├── createUser(UserDto) → save → toDto
│       ├── getUserById(id)     → findById → toDto or null
│       ├── updateUser(id, dto) → findById → update fields → save
│       └── deleteUser(id)      → findById → delete
│
└── entity/
    └── User
        { id (PK, auto), name, surname, email, address,
          alerting (boolean), energyAlertingThreshold (double) }
        table: users
```

---

### 2.6 device-service

**Port:** 8081 | **PostgreSQL**

```
com.leetjouney.device_service
│
├── controller/
│   └── DeviceController
│       ├── POST   /api/v1/device/create      → 200
│       ├── GET    /api/v1/device/{id}         → 200
│       ├── PUT    /api/v1/device/{id}         → 200
│       ├── DELETE /api/v1/device/{id}         → 204
│       └── GET    /api/v1/device/user/{userId}→ 200 List
│
├── service/
│   └── DeviceService
│       ├── getDeviceById(id)              → findById or DeviceNotFoundException
│       ├── createDevice(DeviceDto)        → save → mapToDto
│       ├── updateDevice(id, DeviceDto)    → findById → update → save
│       ├── deleteDevice(id)               → existsById → deleteById
│       └── getAllDevicesByUserId(userId)   → findAllByUserId → mapToList
│
└── entity/
    └── Device
        { id (PK, auto), name,
          type (DeviceType enum: SPEAKER/CAMERA/THERMOSTAT/LIGHT/LOCK/DOORBELL),
          location, userId (FK reference, not JPA join) }
        table: device
```

---

### 2.7 insight-service

**Port:** 8085 | **No DB** | **Spring Boot 3.5 + Spring AI**

```
com.leetjourney.insight_service
│
├── controller/
│   └── InsightController
│       ├── GET /api/v1/insight/saving-tips/{userId}
│       └── GET /api/v1/insight/overview/{userId}
│
├── service/
│   └── InsightService
│       ├── Dependencies:
│       │   ├── UsageClient (Feign → usage-service :8083)
│       │   └── OllamaChatModel (Spring AI)
│       │
│       ├── getSavingsTips(userId)
│       │   ├── usageClient.getXDaysUsageForUser(userId, 3)
│       │   ├── totalUsage = sum(device.energyConsumed)
│       │   ├── prompt: "Total energy: {X}. How to reduce? Compare to avg?"
│       │   └── ollamaChatModel.call(prompt) → InsightDto
│       │
│       └── getOverview(userId)
│           ├── usageClient.getXDaysUsageForUser(userId, 3)
│           ├── prompt: "Analyse: {deviceList}. Actionable insights?"
│           └── ollamaChatModel.call(prompt) → InsightDto
│
└── dto/
    └── InsightDto { userId, tips (AI text), energyUsage (double) }

Config:
  spring.ai.ollama.base-url       = http://localhost:11434
  spring.ai.ollama.chat.model     = qwen2.5:3b
  pull-model-strategy             = never
  usage.service.url               = http://localhost:8083/api/v1/usage
```

---

### 2.8 api-gateway

**Port:** 9000 | **No DB** | **Spring Cloud Gateway MVC**

```
com.leetjourney.api_gateway
│
├── config/
│   └── SecurityConfig
│       └── SecurityFilterChain: all /api/v1/** → permitAll
│           (Keycloak OAuth2 configured but currently open)
│
└── route/  (one class per downstream service)
    ├── UserServiceRoutes
    │   /api/v1/user/**  → localhost:8080
    │   CB: userServiceCircuitBreaker → /userFallbackRoute
    │
    ├── DeviceServiceRoutes
    │   /api/v1/device/**  → localhost:8081
    │   CB: deviceServiceCircuitBreaker → /fallbackRoute
    │
    ├── IngestionServiceRoutes
    │   /api/v1/ingestion/**  → localhost:8082
    │   CB: ingestionServiceCircuitBreaker → /ingestionFallbackRoute
    │
    ├── UsageServiceRoutes
    │   /api/v1/usage/**  → localhost:8083
    │   CB: usageServiceCircuitBreaker → /usageFallbackRoute
    │
    ├── AlertServiceRoutes
    │   /api/v1/alert/**  → localhost:8084
    │   CB: alertServiceCircuitBreaker → /alertFallbackRoute
    │
    └── InsightServiceRoutes
        /api/v1/insight/**  → localhost:8085
        CB: insightServiceCircuitBreaker → /insightFallbackRoute

Resilience4j Circuit Breaker config:
  slidingWindowType:           COUNT_BASED
  slidingWindowSize:           8
  failureRateThreshold:        20%
  minimumNumberOfCalls:        4
  waitDurationInOpenState:     5s
  permittedCallsInHalfOpen:    2
  autoTransitionToHalfOpen:    true

Swagger aggregation:
  /swagger-ui.html  aggregates all 6 service API docs
  /docs/{svc}/v3/api-docs  → rewrite /v3/api-docs on each service
```

---

### 2.9 Kafka Topics Design

```
┌─────────────────────────────────────────────────────────────────────┐
│                      KAFKA TOPICS                                   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Topic: energy-usage                                                │
│  ─────────────────────────────────────────────────────────         │
│  Partitions:           5                                            │
│  Replication Factor:   1                                            │
│  Partition Strategy:   RoundRobinPartitioner (usage-service)        │
│  Message Format:       JSON                                         │
│  Retention:            auto (7 days default, DELETE cleanup)        │
│  Segment Size:         ~109 MB (measured under load)               │
│                                                                     │
│  Producer: ingestion-service                                        │
│    key: null (round-robin)                                          │
│    value: EnergyUsageEvent                                          │
│    { deviceId: Long, energyConsumed: double,                        │
│      timestamp: Instant (ISO string) }                              │
│                                                                     │
│  Consumer Group: usage-service                                      │
│    concurrency: 5 (one thread per partition)                        │
│    ack-mode: manual                                                 │
│    max.poll.records: 500                                            │
│    session.timeout.ms: 60000                                        │
│    Offset committed: ONLY after InfluxDB write succeeds             │
│                                                                     │
│  Measured stats:                                                    │
│    Total messages:  1,091,258+                                      │
│    Consumer lag:    0 (fully consumed)                              │
│    Distribution:    ~even across 5 partitions                       │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Topic: energy-alerts                                               │
│  ─────────────────────────────────────────────────────────         │
│  Partitions:           5                                            │
│  Replication Factor:   1                                            │
│  Partition Strategy:   RoundRobinPartitioner (usage-service)        │
│  Message Format:       JSON                                         │
│                                                                     │
│  Producer: usage-service (scheduler, every 5s)                     │
│    key: null (round-robin)                                          │
│    value: AlertingEvent                                             │
│    { userId, message, threshold, energyConsumed, email,             │
│      totalKwh, estimatedCost, projectedMonthlyCost,                 │
│      alertLevel (WARNING/CRITICAL), deviceName }                    │
│                                                                     │
│  Consumer Group: alert-service                                      │
│    concurrency: 5                                                   │
│    max.poll.records: 500                                            │
│    fetch.max.wait.ms: 100                                           │
│                                                                     │
│  Alert production conditions:                                       │
│    1. totalConsumption > user.energyAlertingThreshold               │
│    2. lastAlertTime[userId] > 1 hour ago (cooldown)                 │
│    3. user.alerting == true                                         │
│                                                                     │
│  Measured stats:                                                    │
│    Total messages: 10,599                                           │
│    Consumer lag:   0 (fully consumed)                               │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Internal Topics (auto-created by Kafka):                           │
│  __consumer_offsets  50 partitions  (offset tracking)              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

### 2.10 Database Schemas

```
PostgreSQL: home_energy_tracker (port 5433)
═══════════════════════════════════════════

TABLE: users
┌─────────────────────────┬───────────────┬──────────────────────────┐
│ Column                  │ Type          │ Notes                    │
├─────────────────────────┼───────────────┼──────────────────────────┤
│ id                      │ BIGINT (PK)   │ AUTO_INCREMENT           │
│ name                    │ VARCHAR       │                          │
│ surname                 │ VARCHAR       │                          │
│ email                   │ VARCHAR       │                          │
│ address                 │ VARCHAR       │                          │
│ alerting                │ BOOLEAN       │ enable/disable alerts    │
│ energy_alerting_        │ DOUBLE        │ Watts threshold          │
│   threshold             │               │                          │
└─────────────────────────┴───────────────┴──────────────────────────┘
Service: user-service

TABLE: device
┌─────────────────────────┬───────────────┬──────────────────────────┐
│ Column                  │ Type          │ Notes                    │
├─────────────────────────┼───────────────┼──────────────────────────┤
│ id                      │ BIGINT (PK)   │ AUTO_INCREMENT           │
│ name                    │ VARCHAR       │                          │
│ type                    │ VARCHAR       │ SPEAKER/CAMERA/THERMOSTAT│
│                         │               │ LIGHT/LOCK/DOORBELL      │
│ location                │ VARCHAR       │                          │
│ user_id                 │ BIGINT        │ FK to users.id (logical) │
└─────────────────────────┴───────────────┴──────────────────────────┘
Service: device-service

TABLE: alert
┌─────────────────────────┬───────────────┬──────────────────────────┐
│ Column                  │ Type          │ Notes                    │
├─────────────────────────┼───────────────┼──────────────────────────┤
│ id                      │ BIGINT (PK)   │ AUTO_INCREMENT           │
│ user_id                 │ BIGINT        │                          │
│ created_at              │ TIMESTAMP     │                          │
│ sent                    │ BOOLEAN       │ email delivery status    │
└─────────────────────────┴───────────────┴──────────────────────────┘
Service: alert-service

TABLE: complaints
┌─────────────────────────┬───────────────┬──────────────────────────┐
│ Column                  │ Type          │ Notes                    │
├─────────────────────────┼───────────────┼──────────────────────────┤
│ id                      │ BIGINT (PK)   │ AUTO_INCREMENT           │
│ user_id                 │ BIGINT        │                          │
│ title                   │ VARCHAR       │                          │
│ description             │ VARCHAR(2000) │                          │
│ category                │ VARCHAR       │ BILLING/TECHNICAL/       │
│                         │               │ SERVICE/OTHER            │
│ status                  │ VARCHAR       │ OPEN/IN_PROGRESS/        │
│                         │               │ RESOLVED/REJECTED        │
│ priority                │ VARCHAR       │ LOW/MEDIUM/HIGH/CRITICAL │
│ image_keys              │ TEXT          │ comma-separated S3 keys  │
│ admin_response          │ VARCHAR(2000) │                          │
│ created_at              │ TIMESTAMP     │ set by @PrePersist       │
│ updated_at              │ TIMESTAMP     │ set by @PreUpdate        │
└─────────────────────────┴───────────────┴──────────────────────────┘
Service: complaint-service
```

---

### 2.11 InfluxDB Schema

```
InfluxDB 2.x (port 8072)
org:    leetjourney
bucket: usage-bucket
retention: 1 week

Measurement: energy_usage
─────────────────────────
Tags (indexed):
  deviceId    string    e.g. "1", "2", "3", "4", "5"

Fields (not indexed):
  energyConsumed  double    Watts, range 0.0–4.0

Timestamp:
  precision: milliseconds (WritePrecision.MS)
  set by: Instant.now() at consumer receipt time

Sample Line Protocol:
  energy_usage,deviceId=1 energyConsumed=1.42 1754352000000

Query example (1-hr rolling window, used by scheduler):
  from(bucket: "usage-bucket")
    |> range(start: time(v: "2026-08-05T01:00:00Z"),
             stop:  time(v: "2026-08-05T02:00:00Z"))
    |> filter(fn: (r) => r["_measurement"] == "energy_usage")
    |> filter(fn: (r) => r["_field"] == "energyConsumed")
    |> group(columns: ["deviceId"])
    |> sum(column: "_value")

Result: one row per deviceId with total Watts for last 1 hour

Measured data volume:
  ~433,938 data points across 5 devices
  Even distribution: ~86-87K per device
  Variance: < 1% (confirms round-robin is working)
```

---

### 2.12 AWS S3 Key Structure

```
Bucket: home-energy-tracker-complaints
Region: ap-south-1 (Mumbai)
Auth:   AWS SDK v2, StaticCredentialsProvider

Key format:
  users/{userId}/complaints/{complaintId}/{uuid}-{originalFilename}

Example:
  users/4/complaints/89/f39abc12-0d48-4b2d-a983-sunrise-86008.jpg

Folder structure:
  users/
  ├── 1/
  │   └── complaints/
  │       ├── 101/
  │       │   ├── {uuid}-image1.jpg
  │       │   └── {uuid}-image2.jpg
  │       └── 106/
  │           └── {uuid}-video.mp4
  ├── 2/
  │   └── complaints/
  │       └── 102/ ...
  └── ...

Metadata per object:
  uploaded-by:       home-energy-tracker
  module:            complaint-service
  original-file-name:{original filename}
  user-id:           {userId}
  complaint-id:      {complaintId}

imageKeys in DB:
  "users/4/complaints/89/{uuid}-img1.jpg,users/4/complaints/89/{uuid}-img2.jpg"
  (comma-separated, stored as TEXT in complaints.image_keys)

Upload flow: async (CompletableFuture.runAsync + parallelStream)
  API response time: ~5s (Spring reads file bytes into memory)
  S3 upload: background, parallel, after response sent
  Max file size:   50 MB per file
  Max request:     100 MB total
```

---

### 2.13 Sequence Diagrams

#### Ingestion → InfluxDB (happy path)

```
Simulator    IngestionController  IngestionService   Kafka          UsageService    InfluxDB
    │                │                  │              │                 │              │
    │─POST /ingest──►│                  │              │                 │              │
    │                │─ingestEnergy()──►│              │                 │              │
    │                │                  │─send(event)─►│                 │              │
    │                │                  │              │─@KafkaListener─►│              │
    │                │                  │◄─whenComplete│                 │              │
    │                │                  │ successCnt++ │                 │─writePoint()─►│
    │◄─201 Created───│                  │              │                 │              │
    │                │                  │              │                 │◄─confirmed───│
    │                │                  │              │                 │─ack()────────►│
    │                │                  │              │◄─offset commit──│              │
```

#### Aggregation → Alert Email

```
Scheduler    UsageService    DeviceService   UserService    Kafka         AlertService   EmailService
    │              │               │              │            │                │              │
    │─every 5s────►│               │              │            │                │              │
    │              │─Flux query────────────────────────────────────────────────────────────────►InfluxDB
    │              │◄──results─────────────────────────────────────────────────────────────────│
    │              │─GET /device/{id}►│              │            │                │              │
    │              │◄──DeviceDto──────│              │            │                │              │
    │              │─GET /user/{id}──────────────────►│           │                │              │
    │              │◄──UserDto (threshold, email)──────│           │                │              │
    │              │                  │               │            │                │              │
    │              │─cooldown check   │               │            │                │              │
    │              │─threshold check  │               │            │                │              │
    │              │─build AlertingEvent               │            │                │              │
    │              │─send(AlertingEvent)───────────────────────────►│                │              │
    │              │─lastAlertTime.put(userId, now)    │            │                │              │
    │              │                  │               │            │─@KafkaListener─►│              │
    │              │                  │               │            │                │─@Async────────►│
    │              │                  │               │            │                │              │─SMTP→Resend
    │              │                  │               │            │                │◄─sent────────│
    │              │                  │               │            │                │─AlertRepo.save│
```

#### Complaint Upload

```
Client      ComplaintController   ComplaintService   PostgreSQL   AsyncThread    S3Client
   │               │                    │               │              │             │
   │─POST /compl──►│                    │               │              │             │
   │               │─createComplaint()─►│               │              │             │
   │               │                   │─save(complaint)►│             │             │
   │               │                   │◄──savedId(89)───│             │             │
   │               │                   │─runAsync()──────────────────►│             │
   │◄─200 OK───────│                   │               │              │             │
   │               │                   │               │              │─parallelStream
   │               │                   │               │              │─uploadFile()─►│
   │               │                   │               │              │              │─PutObject
   │               │                   │               │              │              │  (async)
   │               │                   │               │              │◄─S3 key──────│
   │               │                   │               │              │─save(imageKeys)►│
   │               │                   │               │◄─updated─────│             │
```

#### AI Insight

```
Client     InsightController   InsightService   UsageService   Ollama(:11434)
   │              │                 │               │                │
   │─GET /tips────►│                 │               │                │
   │               │─getSavingsTips()►│               │                │
   │               │                 │─GET /usage/───►│                │
   │               │                 │  {id}?days=3   │                │
   │               │                 │◄──UsageDto──────│                │
   │               │                 │─build prompt    │                │
   │               │                 │─call(prompt)────────────────────►│
   │               │                 │                 │  qwen2.5:3b   │
   │               │                 │◄────AI text─────────────────────│
   │◄──InsightDto──│                 │               │                │
```

---

*Document generated from actual source code — all class names, method signatures, port numbers, and config values are verified against the codebase.*
