# Home Energy Tracker

[![Java](https://img.shields.io/badge/Java-21-orange.svg)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.0-green.svg)](https://spring.io/projects/spring-boot)
[![Spring Cloud](https://img.shields.io/badge/Spring%20Cloud-2025.1.0-blue.svg)](https://spring.io/projects/spring-cloud)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-231F20.svg?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![InfluxDB](https://img.shields.io/badge/InfluxDB-2.x-22ADF6.svg?logo=influxdb&logoColor=white)](https://www.influxdata.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![Prometheus](https://img.shields.io/badge/Prometheus-Alerting-E6522C.svg?logo=prometheus&logoColor=white)](https://prometheus.io/)
[![Grafana](https://img.shields.io/badge/Grafana-Dashboards-F46800.svg?logo=grafana&logoColor=white)](https://grafana.com/)
[![CI](https://github.com/zexxitywave/home-energy-tracker/actions/workflows/cicd.yml/badge.svg)](https://github.com/zexxitywave/home-energy-tracker/actions/workflows/cicd.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A **production-grade microservices system** for real-time household energy monitoring. Ingests device readings at **606 msg/s peak**, processes them through a **3-stage Kafka pipeline** with **1.09M+ messages**, persists to **InfluxDB** (129K+ data points, 155 MB), classifies **WARNING/CRITICAL** alerts via 1-hour rolling aggregations, and delivers billing projections through async email — all observable via **Prometheus + Grafana** with **4 dashboards**, **19 alert rules**, and **p95 < 27 ms** HTTP latency at **100% success rate**.

---

## Table of Contents

- [Architecture](#architecture)
- [Services](#services)
- [Tech Stack](#tech-stack)
- [Kafka Pipeline](#kafka-pipeline)
- [Observability](#observability)
- [Load Testing Results](#load-testing-results)
- [JVM Metrics Under Load](#jvm-metrics-under-load)
- [InfluxDB Data](#influxdb-data)
- [Getting Started](#getting-started)
- [API Reference](#api-reference)
- [Access Points](#access-points)
- [Project Structure](#project-structure)
- [Key Design Decisions](#key-design-decisions)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        CLIENT / POSTMAN                             │
└────────────────────────────┬────────────────────────────────────────┘
                             │ HTTP
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    API GATEWAY  :9000                               │
│         Spring Cloud Gateway · Circuit Breaker (Resilience4j)      │
│               OAuth2 Resource Server (Keycloak JWT)                │
└───┬──────────────┬───────────────┬──────────────┬───────────────────┘
    │              │               │              │
    ▼              ▼               ▼              ▼
┌────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐
│  user  │   │  device  │   │ingestion │   │ insight  │
│ :8080  │   │  :8081   │   │  :8082   │   │  :8085   │
└────┬───┘   └────┬─────┘   └────┬─────┘   └──────────┘
     │            │              │
     ▼            ▼              │ Kafka Producer
┌─────────────────────┐         │ topic: energy-usage
│     PostgreSQL      │         │ (5 partitions, KRaft)
│  home_energy_tracker│         ▼
└─────────────────────┘  ┌─────────────────────────────┐
                         │      usage-service :8083     │
                         │  Kafka Consumer (5 threads)  │
                         │  Synchronous InfluxDB write  │
                         │  Scheduled 5s Flux query     │
                         │  WARNING/CRITICAL classifier │
                         └──────┬──────────┬────────────┘
                                │          │
                                ▼          ▼
                         ┌──────────┐  Kafka Producer
                         │ InfluxDB │  topic: energy-alerts
                         │  :8072   │  (5 partitions)
                         │ 129K pts │       │
                         │  155 MB  │       ▼
                         └──────────┘  ┌─────────────────────┐
                                       │  alert-service :8084 │
                                       │  Kafka Consumer      │
                                       │  5 concurrent threads│
                                       │  Async email (SMTP)  │
                                       │  10,599 alerts sent  │
                                       └──────────┬───────────┘
                                                  │
                                                  ▼
                                           ┌─────────────┐
                                           │   Mailpit   │
                                           │   :8025     │
                                           └─────────────┘
```

### Architecture Diagrams

| Diagram | Preview |
|---------|---------|
| Full Microservices Flow | ![Full flow](diagrams/full-microservices-flow-diagram-with-components.png) |
| Circuit Breaker (API Gateway) | ![Circuit breaker](diagrams/circuit-breaker-in-api-gateway.png) |
| Network Separation | ![Network](diagrams/diagram-showing-gateway-in-public-network.png) |
| Observability Stack | ![Observability](diagrams/observability-with-prometheus-and-grafana.png) |
| Background & Requirements | ![Background](diagrams/background-and-requirements.png) |

---

## Services

| Service | Port | Responsibility |
|---------|------|----------------|
| **api-gateway** | `9000` | Single entry point — routing, circuit breaking, JWT validation |
| **user-service** | `8080` | User accounts, alerting preferences, energy thresholds |
| **device-service** | `8081` | Device registry — CRUD for smart meters and plugs |
| **ingestion-service** | `8082` | Accept energy readings via HTTP, produce to Kafka `energy-usage` |
| **usage-service** | `8083` | Consume readings → write to InfluxDB → aggregate → produce alerts |
| **alert-service** | `8084` | Consume alerts → send WARNING/CRITICAL email with billing projection |
| **insight-service** | `8085` | AI-powered usage insights via Spring AI + Ollama (optional) |
| **complaint-service** | `8086` | Complaint management with parallel S3 multi-file upload (AWS Mumbai) |

---

## Tech Stack

| Category | Technology |
|----------|-----------|
| Language | Java 21 |
| Framework | Spring Boot 4.0 (all services), Spring Boot 3.5 (insight-service) |
| Messaging | Apache Kafka (KRaft mode, no ZooKeeper), 5 partitions per topic |
| Time-series DB | InfluxDB 2.x (Flux query language), 1-week retention |
| Relational DB | PostgreSQL (users, devices, alerts) |
| Cloud Storage | AWS S3 (ap-south-1) — complaint file uploads |
| API Gateway | Spring Cloud Gateway Server WebMVC + Resilience4j circuit breakers |
| Security | Keycloak (OAuth2/JWT), Spring Security OAuth2 Resource Server |
| Observability | Micrometer + Prometheus + Grafana (4 dashboards, 19 alert rules) |
| Email (dev) | Mailpit (SMTP trap, web UI at :8025) |
| Containerization | Docker + Docker Compose |
| Build | Maven (per-service `mvnw` wrapper) |
| AI/ML | Spring AI + Ollama (insight-service) |

---

## Kafka Pipeline

The core data flow is a **3-stage Kafka pipeline** running in **KRaft mode** (no ZooKeeper):

```
ingestion-service                usage-service                alert-service
      │                               │                             │
      │  POST /api/v1/ingestion        │                             │
      │  (built-in mock simulator)    │                             │
      │ ──────────────────►           │                             │
      │                               │                             │
      │  produce →  energy-usage      │                             │
      │ ═══════════════════════════►  │                             │
      │      (5 partitions)           │  consume (5 threads)        │
      │      RoundRobin               │  sync InfluxDB write        │
      │                               │  manual Kafka ack           │
      │                               │  scheduled 5s Flux query   │
      │                               │  threshold classification   │
      │                               │                             │
      │                               │  produce → energy-alerts    │
      │                               │ ════════════════════════►   │
      │                               │      (5 partitions)         │  consume (5 threads)
      │                               │                             │  async email send
      │                               │                             │  PostgreSQL save
```

---

## Load Testing Results

### Ingestion Service — Kafka Pipeline Load Test

All tests run locally on a single machine with the full Docker Compose stack running (Kafka, InfluxDB, PostgreSQL, Mailpit, Prometheus, Grafana) plus all 5 microservices on the host JVM.

#### Peak Load Summary

| Metric | Value |
|--------|-------|
| Peak ingestion rate | **606 req/s** |
| Mean ingestion rate | **285 req/s** |
| Total Kafka messages (`energy-usage`) | **1,091,258** |
| Total Kafka messages (`energy-alerts`) | **10,599** |
| `energy-usage` topic size | **155 MB** |
| `energy-alerts` topic size | **4 MB** |
| Ingestion success rate | **100%** (0 failed) |
| Total failed messages | **0** |
| HTTP P95 latency (ingestion endpoint) | **26.135 ms** |
| HTTP P50 latency (ingestion endpoint) | **< 1 ms** |
| HTTP status 200 peak rate | **606 req/s** |
| HTTP status 201 peak rate | **1.06K req/s** |
| InfluxDB data points written | **129,500+** |
| InfluxDB total stored data | **155 MB** |
| Kafka partitions per topic | **5** |
| Kafka partition strategy | **Round-robin** |
| Usage-service consumer threads | **5 concurrent** |
| Alert-service consumer threads | **5 concurrent** |
| Kafka ack mode | **Manual (after InfluxDB write)** |
| Alert cooldown per user | **1 hour** |

#### Ingestion Rate Over Time

```
 600 ┤                    ╭╮
 500 ┤                   ╭╯╰╮
 400 ┤                  ╭╯  ╰╮
 300 ┤            ╭─────╯    ╰────── 285 avg
 200 ┤       ╭────╯
 100 ┤  ╭────╯
   0 ┼──╯
      t=0     t+5m    t+10m   t+15m
```

#### Alert Triggering Logic

```
Usage consumed (1hr rolling window) > user threshold
  └── < 1.5x threshold  →  WARNING alert  → email via Mailpit
  └── ≥ 1.5x threshold  →  CRITICAL alert → email via Mailpit
  └── cooldown active (< 1hr since last alert) → skipped
```

**10,599 alerts triggered** across the full test run, stored in both Kafka (`energy-alerts`) and PostgreSQL.

---

### Complaint Service — AWS S3 Load Test

All uploads went to **AWS S3 (ap-south-1)** with parallel processing via `CompletableFuture` + `parallelStream`.

| # | Scenario | Users | Files | Total Payload | Requests | Error % | API Response |
|---|----------|-------|-------|---------------|----------|---------|--------------|
| 1 | Single user, bulk payload | 1 | 307 files | ~91 MB | 1 | 0% | 5.17 s |
| 2 | Single user, 300 images | 1 | 300 images | ~50 MB | 300 | 0% | sub-2 s each |
| 3 | Multi-user JMeter load test | 5 users | 2 files each | — | 1,450 | 0% | 40 ms median |

#### JMeter Detailed Results (1,450 requests, 5 concurrent users)

| Metric | Value |
|--------|-------|
| Total Requests | 1,450 |
| Error Rate | **0.00%** |
| Average Response Time | 250 ms |
| Median (P50) | **40 ms** |
| P90 | 396 ms |
| P95 | 1,429 ms |
| P99 | 4,119 ms |
| Max Response Time | 5,688 ms |
| Throughput | **5.9 req/s** |

---

## JVM Metrics Under Load

Measured on `usage-service` during the 1.09M+ message load test. `usage-service` is the highest-pressure service due to its `@Scheduled(fixedRate=5000)` aggregation loop creating short-lived objects every 5 seconds.

### Heap & Memory

| Metric | Value | Health |
|--------|-------|--------|
| Heap Used | ~57 MB | Very good |
| Heap Max | ~2 GB | Plenty of headroom |
| Non-Heap (Metaspace + CodeCache) | ~90 MB | Normal |
| Heap Used % | ~3% | Excellent |

### Garbage Collection

| Metric | Value | Health |
|--------|-------|--------|
| GC Type | G1GC (default JDK 21) | — |
| Minor GC frequency | ~1.3 / min | Normal |
| GC Pause Time | ~0.09 ms avg | Excellent |
| GC Pause P95 | < 1 ms | Excellent |
| GC Pause P99 | < 5 ms | Good |
| Heap Allocation Rate | spikes every 5s (scheduler) | Expected |
| Memory Promoted to Old Gen | minimal | No leak detected |

> The 5-second spikes in allocation rate directly correlate with `aggregateDeviceEnergyUsage()` — it creates `List<FluxTable>`, `List<DeviceEnergy>`, `Map<Long, List<...>>` per scheduler tick. All are short-lived and cleaned up by Minor GC without promoting to Old Gen.

### Threads

| Metric | Value | Health |
|--------|-------|--------|
| Live Threads | ~50 | Normal |
| Daemon Threads | ~35 | Normal |
| Blocked Threads | **0** | Excellent |
| Peak Threads | ~55 | Normal |
| Kafka Consumer Threads | 5 | Configured |

### HikariCP (usage-service does not use SQL, applies to user/device/alert services)

| Metric | Value | Health |
|--------|-------|--------|
| Active Connections | 1–3 | Normal |
| Idle Connections | 7–9 | Normal |
| Pending Connections | **0** | Excellent |
| P95 Acquisition Time | < 5 ms | Good |

---

## Observability

Four production-grade Grafana dashboards auto-provisioned on startup via `/docker/grafana/provisioning/`.

### Dashboards

#### 1. Overview Dashboard (`het-overview.json`) — 33 panels

**System Health row**
- Services Online (stat, green/yellow/red threshold)
- Services Offline (stat, green/red)
- Total HTTP Req/s all services (stat)
- 5xx Error Rate (stat)
- P95 Latency all services (stat)
- Total JVM Heap Used (stat)

**Service Up/Down Status row**
- Per-service UP/DOWN status (stat, horizontal, color-coded)

**HTTP Traffic row**
- HTTP Request Rate by Service (timeseries)
- HTTP Request Rate by Status Code (timeseries, 2xx green / 4xx yellow / 5xx red)
- P50/P95/P99 Latency all services (timeseries)
- P95 Latency per Service (timeseries)

**JVM Memory & CPU row**
- JVM Heap Used per Service (timeseries)
- JVM Heap Used vs Max per Service (bar gauge, horizontal, 70%/85% thresholds)
- CPU Usage per Service — system + process (timeseries)
- JVM GC Pause Time per Service (timeseries)

**Threads & DB Connections row**
- JVM Live Threads per Service (timeseries)
- HikariCP Connection Pool — active / idle / pending (timeseries, color-coded)

**Top Endpoints row**
- Top 10 Busiest Endpoints (table, sorted by req/s)
- Top 10 Slowest Endpoints P95 (table, sorted by latency)

**GC Overview (all services) row**
- GC Pause Time / s (timeseries)
- GC Collections / min (timeseries)
- Heap Allocation Rate bytes/s (timeseries)

**usage-service GC Spotlight row**
- Heap Used vs Max (timeseries, max shown as red dashed line)
- GC Pause by action + cause (timeseries)
- Memory Promoted to Old Gen vs Allocated (timeseries)

---

#### 2. Service Health Dashboard (`het-service-health.json`) — per-service deep dive

- Service dropdown variable (all services)
- Request rate by HTTP method (GET/POST/PUT/DELETE)
- 4xx and 5xx breakdown by endpoint
- Latency heatmap (P50/P95/P99)
- JVM heap vs non-heap, buffer pools (direct/mapped)
- GC collections/min and pause duration
- Thread states (runnable / waiting / blocked — blocked in red)
- HikariCP P95 acquisition time

---

#### 3. Kafka & Ingestion Pipeline Dashboard (`het-kafka-pipeline.json`)

**Ingestion Scorecards**
- Total Messages Sent (counter)
- Total Failed Messages (counter, red if > 0)
- Ingestion Rate ms (stat)
- Success Rate % (stat, green at 100%)
- Ingestion Service HTTP req/s (stat)
- Ingestion P95 Latency ms (stat)

**Ingestion Throughput**
- Success vs Failed message rate (timeseries)
- Cumulative Messages Sent (timeseries, total counter)
- HTTP Rate by Status Code (timeseries)
- Ingestion Latency P50/P95/P99 (timeseries)

**Usage-service & Alert-service panels**
- Usage-service GC pressure and blocked threads
- Alert-service heap used vs max (with threshold line)
- All-services request rate comparison
- All-services CPU comparison

---

#### 4. JVM GC Deep Dive Dashboard (`het-jvm-gc.json`) — 25 panels, service variable

**GC Scorecards row**
- GC Pause Time avg (stat, thresholds at 50ms/200ms)
- GC Collections / min (stat, thresholds at 10/30)
- Heap Allocation Rate bytes/s (stat, thresholds at 5MB/20MB)
- Memory Promoted to Old Gen / s (stat)
- Heap Used % (gauge, thresholds at 70%/85%)
- Non-Heap (Metaspace) Used (stat, threshold at 128MB)

**GC Pause Analysis row**
- GC Pause Time by action & cause (timeseries)
- GC Collections / min by action (timeseries)
- GC Pause P99 stop-the-world (timeseries)
- GC Pause P50/P95/P99 combined (timeseries)

**Heap Memory — Allocation & Promotion row**
- Heap Used vs Max (timeseries, max as red dashed)
- Allocation Rate vs Promotion Rate (timeseries, promoted in orange)
- Heap Memory Pool Breakdown — used per pool (timeseries)
- Non-Heap — Metaspace / CodeCache Used (timeseries)

**usage-service Scheduler GC Correlation row**
- GC Pause + Allocation Rate dual-axis (correlates 5s scheduler spikes)
- Heap % gauge (green/yellow/red)

**Threads & Buffer Pools row**
- Thread States — blocked/runnable/waiting (timeseries, color-coded)
- Buffer Pools — direct/mapped used vs capacity (timeseries)
- Loaded vs Unloaded Classes (timeseries)
- Live / Daemon / Peak Threads (timeseries)

---

### Prometheus Alert Rules (19 rules across 6 groups)

All rules in `docker/prometheus/alert-rules.yml`, evaluated every 15 seconds.

#### Group 1: `service-availability`

| Alert | Expression | For | Severity |
|-------|-----------|-----|----------|
| `ServiceDown` | `up == 0` | 30s | critical |
| `ServiceRestartDetected` | `increase(process_uptime_seconds[2m]) < 0` | 0s | warning |

#### Group 2: `http-errors`

| Alert | Expression | Threshold | For | Severity |
|-------|-----------|-----------|-----|----------|
| `HighHttpErrorRate5xx` | `rate(http_server_requests_seconds_count{status=~"5.."}[2m])` | > 0.5 req/s | 1m | critical |
| `ElevatedHttpErrorRate4xx` | `rate(http_server_requests_seconds_count{status=~"4.."}[2m])` | > 5 req/s | 2m | warning |
| `HighP95Latency` | `histogram_quantile(0.95, ...)` | > 2s | 2m | warning |
| `CriticalP99Latency` | `histogram_quantile(0.99, ...)` | > 5s | 2m | critical |
| `ZeroRequestRate` | `rate(http_server_requests_seconds_count[5m]) == 0 and up == 1` | — | 3m | warning |

#### Group 3: `jvm-memory`

| Alert | Expression | Threshold | For | Severity |
|-------|-----------|-----------|-----|----------|
| `HighJvmHeapUsage` | `jvm_memory_used_bytes / jvm_memory_max_bytes` | > 80% | 2m | warning |
| `CriticalJvmHeapUsage` | `jvm_memory_used_bytes / jvm_memory_max_bytes` | > 92% | 1m | critical |
| `FrequentGCPauses` | `rate(jvm_gc_pause_seconds_count[2m]) * 60` | > 10/min | 2m | warning |
| `LongGCPauseDuration` | `rate(jvm_gc_pause_seconds_sum[2m])` | > 0.3 s/s | 2m | warning |

#### Group 4: `jvm-threads`

| Alert | Expression | Threshold | For | Severity |
|-------|-----------|-----------|-----|----------|
| `HighBlockedThreadCount` | `jvm_threads_states_threads{state="blocked"}` | > 10 | 1m | warning |
| `ThreadCountSpike` | `jvm_threads_live_threads` | > 200 | 2m | warning |

#### Group 5: `hikaricp`

| Alert | Expression | Threshold | For | Severity |
|-------|-----------|-----------|-----|----------|
| `HikariConnectionPoolExhausted` | `hikaricp_connections_pending` | > 0 | 30s | warning |
| `HikariConnectionPoolCritical` | `hikaricp_connections_pending` | > 5 | 30s | critical |
| `HighConnectionAcquisitionTime` | `histogram_quantile(0.95, hikaricp_connections_acquire_seconds_bucket)` | > 0.5s | 2m | warning |

#### Group 6: `ingestion-pipeline`

| Alert | Expression | Threshold | For | Severity |
|-------|-----------|-----------|-----|----------|
| `IngestionStalled` | `rate(ingestion_messages_success_total[3m]) == 0 and up == 1` | — | 3m | critical |
| `IngestionFailureDetected` | `rate(ingestion_messages_failed_total[1m])` | > 0 | 30s | warning |
| `IngestionHighErrorRatio` | `failed / (success + failed)` | > 5% | 2m | critical |

---

### Custom Micrometer Metrics (usage-service)

Registered at startup via `MeterRegistry` in `UsageService`:

| Metric Name | Type | Description |
|-------------|------|-------------|
| `usage.events.consumed` | Counter | Total `energy-usage` Kafka events consumed |
| `usage.influx.writes` | Counter | Total points written to InfluxDB |
| `usage.alerts.produced` | Counter | Total alerts published to `energy-alerts` |
| `usage.alerts.warning` | Counter | Total WARNING-level alerts |
| `usage.alerts.critical` | Counter | Total CRITICAL-level alerts |

All exposed at `/actuator/prometheus` and scraped by Prometheus every 15s.

---

### Prometheus Scrape Targets

All services scraped at `/actuator/prometheus`, interval 15s:

| Job | Target |
|-----|--------|
| `user-service` | `host.docker.internal:8080` |
| `device-service` | `host.docker.internal:8081` |
| `ingestion-service` | `host.docker.internal:8082` |
| `usage-service` | `host.docker.internal:8083` |
| `alert-service` | `host.docker.internal:8084` |
| `insight-service` | `host.docker.internal:8085` |
| `api-gateway` | `host.docker.internal:9000` |

Services run on the host; Prometheus runs in Docker. `host.docker.internal` resolves to the host machine via `extra_hosts` in `docker-compose.yml`.

---

## InfluxDB Data

- **Bucket:** `usage-bucket`
- **Org:** `leetjourney`
- **Retention:** 1 week
- **Measurement:** `energy_usage`
- **Tags:** `deviceId`
- **Fields:** `energyConsumed` (double, watts)

### Useful Flux Queries

**Total records stored:**
```flux
from(bucket: "usage-bucket")
  |> range(start: -7d)
  |> filter(fn: (r) => r["_measurement"] == "energy_usage")
  |> filter(fn: (r) => r["_field"] == "energyConsumed")
  |> count()
  |> sum(column: "_value")
```

**Records per device:**
```flux
from(bucket: "usage-bucket")
  |> range(start: -7d)
  |> filter(fn: (r) => r["_measurement"] == "energy_usage")
  |> filter(fn: (r) => r["_field"] == "energyConsumed")
  |> group(columns: ["deviceId"])
  |> count()
```

**Distinct device count:**
```flux
from(bucket: "usage-bucket")
  |> range(start: -7d)
  |> filter(fn: (r) => r["_measurement"] == "energy_usage")
  |> keep(columns: ["deviceId"])
  |> distinct(column: "deviceId")
  |> count()
```

**1-hour rolling sum per device (same query the scheduler uses):**
```flux
from(bucket: "usage-bucket")
  |> range(start: -1h)
  |> filter(fn: (r) => r["_measurement"] == "energy_usage")
  |> filter(fn: (r) => r["_field"] == "energyConsumed")
  |> group(columns: ["deviceId"])
  |> sum(column: "_value")
```

---

## Getting Started

### Prerequisites

- **JDK 21**
- **Docker** and **Docker Compose**
- **Maven** (optional — each service has `./mvnw`)

### 1. Clone

```bash
git clone https://github.com/zexxitywave/home-energy-tracker.git
cd home-energy-tracker
```

### 2. Start Infrastructure

```bash
docker compose up -d
```

Starts: **Kafka (KRaft)**, **PostgreSQL**, **InfluxDB**, **Mailpit**, **Kafka UI**, **Prometheus**, **Grafana**.

Wait ~30 seconds for all containers to be healthy.

### 3. Build Services

```bash
cd user-service      && ./mvnw -q package -DskipTests && cd ..
cd device-service    && ./mvnw -q package -DskipTests && cd ..
cd ingestion-service && ./mvnw -q package -DskipTests && cd ..
cd usage-service     && ./mvnw -q package -DskipTests && cd ..
cd alert-service     && ./mvnw -q package -DskipTests && cd ..
```

### 4. Start Services

Start each in a separate terminal (or via IntelliJ Services panel):

```bash
cd user-service      && ./mvnw spring-boot:run   # terminal 1
cd device-service    && ./mvnw spring-boot:run   # terminal 2
cd ingestion-service && ./mvnw spring-boot:run   # terminal 3 — mock simulator starts automatically
cd usage-service     && ./mvnw spring-boot:run   # terminal 4
cd alert-service     && ./mvnw spring-boot:run   # terminal 5
```

> Services connect to Kafka on `localhost:9094` (external listener). Docker Compose must be running first.

### 5. Verify Pipeline

```bash
# Send a test reading directly to ingestion-service (no JWT needed)
curl -X POST http://localhost:8082/api/v1/ingestion \
  -H 'Content-Type: application/json' \
  -d '{"deviceId":1,"timestamp":"2026-01-01T12:00:00Z","energyConsumed":1.5}'
```

- **Kafka UI** → http://localhost:8070 — messages in `energy-usage` topic
- **InfluxDB** → http://localhost:8072 — query `usage-bucket` for `energy_usage`
- **Mailpit** → http://localhost:8025 — alert emails appear when threshold exceeded
- **Grafana** → http://localhost:3000 — live dashboards

### 6. Force an Alert

Lower a user's threshold to trigger an alert within the next 5-second scheduler tick:

```sql
UPDATE users SET energy_alerting_threshold = 100, alerting = true WHERE id = 1;
```

Then check Mailpit at http://localhost:8025 for the WARNING/CRITICAL email.

### 7. Check Ingestion Stats

```bash
curl http://localhost:8082/api/v1/ingestion/stats
```

```json
{
  "totalSent": 1091258,
  "successCount": 1091258,
  "failedCount": 0,
  "successRate%": 100
}
```

---

## API Reference

### User Service `:8080`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/user` | Create user |
| `GET` | `/api/v1/user/{id}` | Get user by ID |
| `PUT` | `/api/v1/user/{id}` | Update user |
| `DELETE` | `/api/v1/user/{id}` | Delete user |

### Device Service `:8081`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/device/create` | Register a device |
| `GET` | `/api/v1/device/{id}` | Get device by ID |
| `GET` | `/api/v1/device/user/{userId}` | Get all devices for user |

### Ingestion Service `:8082`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/ingestion` | Submit energy reading |
| `GET` | `/api/v1/ingestion/stats` | Get ingestion statistics |

### Usage Service `:8083`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/v1/usage/{userId}?days=7` | Get usage data for user (N days) |

### Alert Service `:8084`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/v1/alerts/{userId}` | Get alert history for user |

### Complaint Service `:8086`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/complaint` | Submit complaint with file attachments (multipart) |
| `GET` | `/api/v1/complaint/{userId}` | Get complaints for user |

---

## Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| **Grafana** | http://localhost:3000 | admin / admin |
| **Prometheus** | http://localhost:9091 | — |
| **Kafka UI** | http://localhost:8070 | — |
| **Mailpit** | http://localhost:8025 | — |
| **InfluxDB** | http://localhost:8072 | token: `my-token` |
| **Keycloak** | http://localhost:8091 | admin / admin |
| **API Gateway** | http://localhost:9000 | JWT required |

---

## Project Structure

```
home-energy-tracker/
├── docker-compose.yml
├── docker/
│   ├── prometheus/
│   │   ├── prometheus.yml          # 7 scrape targets, 15s interval
│   │   └── alert-rules.yml         # 19 alert rules across 6 groups
│   ├── grafana/
│   │   └── provisioning/
│   │       ├── datasources/        # Prometheus datasource
│   │       └── dashboards/
│   │           ├── dashboards.yml
│   │           └── json/
│   │               ├── het-overview.json          # 33 panels
│   │               ├── het-service-health.json    # per-service deep dive
│   │               ├── het-kafka-pipeline.json    # ingestion pipeline
│   │               └── het-jvm-gc.json            # 25-panel GC deep dive
│   ├── mysql/init.sql
│   └── keycloak/
├── diagrams/
├── user-service/
├── device-service/
├── ingestion-service/
├── usage-service/
├── alert-service/
├── complaint-service/
├── insight-service/
├── api-gateway/
└── AGENTS.md
```

---

## Key Design Decisions

**Kafka KRaft (no ZooKeeper)** — Single broker process, no separate quorum to manage in local dev.

**InfluxDB for usage data** — Time-series readings every second per device are a poor fit for relational storage. Flux makes 1-hour rolling aggregations and `group by deviceId` trivial.

**Manual Kafka ack after InfluxDB write** — Consumer only acknowledges a message after the InfluxDB write succeeds. If InfluxDB is down, no ack → Kafka redelivers → zero data loss.

**Round-robin partitioning** — With 5 partitions and 5 consumer threads, round-robin keeps all threads busy. Key-based partitioning caused all traffic to land on one partition, leaving 4 threads idle.

**Async email in alert-service** — SMTP calls wrapped in `@Async` with a dedicated thread pool so Kafka consumer threads are never blocked waiting on email delivery.

**1-hour alert cooldown per user** — Prevents email flooding when a user stays above threshold for extended periods. Tracked in a `ConcurrentHashMap<Long, Instant>` in-memory.

**5-second aggregation scheduler** — Queries InfluxDB for the last hour of data every 5 seconds, groups by `deviceId`, sums per user, then compares against per-user thresholds. Short-lived objects created per tick are all cleaned by Minor GC (~0.09 ms pause).

---

## Future Improvements

- Frontend SPA — real-time energy charts, alert history, device management
- Kubernetes deployment — Helm charts, HPA, external secrets
- End-to-end tests — contract tests across gateway → services → Kafka → DB
- Multi-tenant support — per-household isolation and billing
- WebSocket push — real-time alert delivery without polling
- Centralized config — Spring Cloud Config or Vault for secrets

---

*Built with Spring Boot 4, Java 21, and the full modern microservices stack.*
