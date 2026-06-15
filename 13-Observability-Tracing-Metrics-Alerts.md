# Observability: Distributed Tracing, Metrics & Alerts

> Bài 13 trong lộ trình Transaction Microservice nâng cao.
> Mục tiêu: Trang bị hệ thống Saga với **3 trụ cột Observability** — Logs, Metrics, Traces. Sau bài này, bạn có thể **debug 1 saga fail trong 30 giây** thay vì 30 phút, và **biết trước** khi hệ thống có vấn đề.

---

## 0. Tại sao Observability quan trọng gấp đôi trong microservices

### Trong monolith
- Bug → 1 process → 1 stack trace
- Logs trong 1 file
- Profiler hoạt động bình thường
- Debug bằng IDE trực tiếp

### Trong microservices
- Bug → có thể ở 1 trong 5 service
- Logs ở 5 nơi
- 1 request đi qua 5 hop (REST, Kafka, DB calls)
- Profiler không thấy "the whole picture"

→ **Logs đơn thuần KHÔNG ĐỦ.** Cần **Tracing** để theo dấu request + **Metrics** để biết trend.

> "Without observability, you don't have a distributed system. You have a distributed mystery."

---

## 1. 3 Trụ cột của Observability

```
┌─────────────────────────────────────────────────────────────┐
│                    OBSERVABILITY                             │
├──────────────┬──────────────────┬──────────────────────────┤
│   LOGS       │     METRICS      │      TRACES              │
│              │                  │                          │
│  Cái gì xảy  │ Bao nhiêu, nhanh│ Request đi đâu, ai gọi  │
│  ra (text)   │ cỡ nào (numbers)│ ai (graph)               │
│              │                  │                          │
│  Detailed    │  Aggregated      │  Causal                  │
│  Verbose     │  Time-series     │  Per-request             │
│              │                  │                          │
│  Loki / ELK  │  Prometheus      │  Jaeger / Tempo          │
└──────────────┴──────────────────┴──────────────────────────┘
```

### Khi nào dùng cái nào

**Logs:** chi tiết event của 1 process. "Tại 10:30:25, OrderService nhận request POST /orders với body...".
**Metrics:** xu hướng theo thời gian. "Trong 5 phút qua, success rate là 99.5%".
**Traces:** đường đi của 1 request. "Order ABC đi qua: Order→Kafka→Inventory→Kafka→Payment→...".

→ Bạn cần **cả ba**, không phải 1 trong 3.

---

## 2. Distributed Tracing — Trọng tâm bài này

### 2.1. Vấn đề

User báo: "Order ABC bị stuck ở status PENDING 10 phút rồi". Bạn cần:
1. Tìm log của Order Service → có
2. Tìm log của Saga Orchestrator → có
3. Tìm log của Inventory → 1 record? hay 2? thuộc order nào?
4. ... 30 phút sau

→ Phải có cách nói "show tôi tất cả log/event liên quan đến **request này** xuyên 5 service".

### 2.2. Lời giải: Trace ID + Span ID

Khi request bắt đầu, sinh **Trace ID** unique. Trace ID này **propagate** qua mọi hop:
- HTTP request → header `traceparent`
- Kafka message → header `traceparent`
- DB query → log với traceId

Trong mỗi service, mỗi unit work là 1 **Span** với **Span ID** riêng. Span con biết parent span.

```
Trace 7b3e1a8d (1 request lifecycle):
  ├─ Span A1: Order Service HTTP POST handler (200ms)
  │   ├─ Span A2: Save Order to DB (10ms)
  │   ├─ Span A3: Save Outbox event (5ms)
  │   └─ Span A4: Return response
  │
  ├─ Span B1: Outbox Relay publish Kafka (15ms)
  │
  ├─ Span C1: Saga Orchestrator receive OrderCreated (180ms)
  │   ├─ Span C2: Save Saga instance
  │   └─ Span C3: Dispatch ReserveStock command
  │
  ├─ Span D1: Inventory Service handle ReserveStock (220ms)
  │   ├─ Span D2: Idempotency check (3ms)
  │   ├─ Span D3: Reserve in DB (15ms)
  │   └─ Span D4: Save reply in Outbox (5ms)
  │
  ... (continues through Payment, Shipping, Order confirm)
```

→ **1 trace = 1 user request = đường đi đầy đủ.**

### 2.3. OpenTelemetry — Industry standard

Trước OpenTelemetry: mỗi vendor 1 protocol (Zipkin, Jaeger, X-Ray, Datadog...). Migrate giữa các tools = đau.

**OpenTelemetry (OTel)** = chuẩn vendor-neutral. Code 1 lần, export đi đâu cũng được.

```
Service code ──► OTel SDK ──► OTLP protocol ──► Jaeger/Tempo/Datadog
                                              (đổi backend ko đổi code)
```

### 2.4. W3C Trace Context — Cách propagate

Trace ID/Span ID propagate qua HTTP header chuẩn:
```http
traceparent: 00-7b3e1a8d4f9c2e0a1234567890abcdef-a1b2c3d4e5f6g7h8-01
              │  │                                │
              │  └─ Trace ID (16 bytes)            └─ Parent Span ID
              └─ version
```

Kafka cũng có headers tương tự. Spring Kafka + Micrometer Tracing tự inject/extract.

---

## 3. Setup OpenTelemetry + Jaeger cho demo

### 3.1. Jaeger đã setup trong Docker Compose

```yaml
jaeger:
  image: jaegertracing/all-in-one:1.60
  ports:
    - "16686:16686"   # UI
    - "4317:4317"     # OTLP gRPC
    - "4318:4318"     # OTLP HTTP
```

### 3.2. Dependencies (đã có ở bài 7)

```xml
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-tracing-bridge-otel</artifactId>
</dependency>
<dependency>
    <groupId>io.opentelemetry</groupId>
    <artifactId>opentelemetry-exporter-otlp</artifactId>
</dependency>
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
```

### 3.3. application.yml mỗi service

```yaml
spring:
  application:
    name: order-service        # ← QUAN TRỌNG: trace UI hiển thị tên này

management:
  tracing:
    sampling:
      probability: 1.0         # Dev: 100%. Prod: 0.01-0.1 (1-10%)
  otlp:
    tracing:
      endpoint: http://localhost:4318/v1/traces

logging:
  pattern:
    correlation: "[${spring.application.name},%X{traceId:-},%X{spanId:-}]"
    level: "%5p ${LOGGING_PATTERN_CORRELATION:}"
```

→ Mọi log line giờ có `[order-service,7b3e1a8d...,a1b2c3d4...]` prefix → grep được theo traceId.

### 3.4. Auto-instrumentation — Spring Boot tự làm

Spring Boot 3 + Micrometer Tracing **tự động** instrument:
- HTTP server (mọi endpoint)
- HTTP client (RestTemplate, WebClient)
- JDBC queries
- Kafka producer/consumer
- Spring Data JPA

→ **Không cần code gì thêm**, traces tự xuất hiện trong Jaeger.

### 3.5. Custom Spans cho business logic

Vẫn cần thêm span thủ công cho các operation business meaningful:

```java
@Service
@RequiredArgsConstructor
public class SagaOrchestrator {

    private final Tracer tracer;

    public void handleReply(SagaReply reply) {
        Span span = tracer.spanBuilder("saga.handle-reply")
            .setAttribute("saga.id", reply.getSagaId())
            .setAttribute("saga.reply.success", reply.isSuccess())
            .startSpan();
        
        try (var scope = span.makeCurrent()) {
            doHandleReply(reply);
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }
}
```

Hoặc đẹp hơn với annotation:
```java
@Observed(name = "saga.handle-reply", contextualName = "Handle Reply")
public void handleReply(SagaReply reply) {
    // ...
}
```

### 3.6. Span attributes nên có cho Saga

```java
span.setAttribute("saga.id", sagaId.toString());
span.setAttribute("saga.order_id", orderId.toString());
span.setAttribute("saga.current_state", currentState.name());
span.setAttribute("saga.next_state", nextState.name());
span.setAttribute("saga.is_compensating", currentState.isCompensating());
span.setAttribute("messaging.kafka.topic", topic);
span.setAttribute("messaging.kafka.partition", partition);
```

→ Trong Jaeger UI có thể filter: "show all sagas in COMPENSATING state".

---

## 4. Query Jaeger — Use cases thực tế

### Use case 1: Tìm trace của order cụ thể

Jaeger UI → Search:
```
Service: order-service
Tags: saga.order_id=abc-123
Lookback: 1h
```

→ Hiển thị toàn bộ trace, click vào để thấy waterfall của 200+ span across 5 services.

### Use case 2: Tìm trace chậm

```
Service: saga-orchestrator
Operation: saga.handle-reply
Min Duration: 5s
```

→ Show traces > 5s. Click vào → thấy span nào chậm (DB query? Kafka publish?).

### Use case 3: Tìm trace có lỗi

```
Service: payment-service
Tags: error=true
```

→ Show traces failed.

### Use case 4: Compare 2 traces

Pick 1 happy path + 1 fail path → side by side → thấy "ở đâu khác biệt".

---

## 5. Metrics với Prometheus + Micrometer

### 5.1. Auto-metrics có sẵn

Spring Boot Actuator + Micrometer tự expose:
- `http_server_requests_seconds` — HTTP latency
- `jvm_memory_used_bytes` — heap usage
- `hikaricp_connections_*` — connection pool
- `kafka_consumer_*` — Kafka lag, fetch
- `kafka_producer_*` — produce latency

→ Endpoint `/actuator/prometheus` expose hết.

### 5.2. Prometheus scrape config

`infra/prometheus/prometheus.yml`:
```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'order-service'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets: ['host.docker.internal:8081']

  - job_name: 'inventory-service'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets: ['host.docker.internal:8082']

  - job_name: 'payment-service'
    static_configs:
      - targets: ['host.docker.internal:8083']

  - job_name: 'shipping-service'
    static_configs:
      - targets: ['host.docker.internal:8084']

  - job_name: 'saga-orchestrator'
    static_configs:
      - targets: ['host.docker.internal:8085']
```

### 5.3. Custom Saga Metrics

Đây là phần **giá trị thực sự** — đo các business metric quan trọng:

```java
@Component
@RequiredArgsConstructor
public class SagaMetrics {

    private final MeterRegistry registry;

    // Counter: tổng sagas started
    public void recordSagaStarted() {
        registry.counter("saga.started.total",
            "saga_type", "OrderSaga").increment();
    }

    // Counter: tổng sagas completed (success)
    public void recordSagaCompleted(Duration duration) {
        registry.counter("saga.completed.total",
            "saga_type", "OrderSaga").increment();
        registry.timer("saga.duration",
            "saga_type", "OrderSaga",
            "outcome", "completed")
            .record(duration);
    }

    // Counter: tổng sagas failed (kèm reason)
    public void recordSagaFailed(String failureReason, Duration duration) {
        registry.counter("saga.failed.total",
            "saga_type", "OrderSaga",
            "failure_reason", failureReason).increment();
        registry.timer("saga.duration",
            "outcome", "failed").record(duration);
    }

    // Gauge: số saga đang active
    public void registerActiveSagasGauge(SagaInstanceRepository repo) {
        Gauge.builder("saga.active.count", repo, r -> r.countActive())
            .register(registry);
    }

    // Timer: thời gian xử lý 1 step
    public void recordStepDuration(SagaState state, Duration duration) {
        registry.timer("saga.step.duration",
            "state", state.name())
            .record(duration);
    }
}
```

### 5.4. Outbox/Inbox metrics

```java
@Component
public class OutboxMetrics {

    public void recordPublished() {
        registry.counter("outbox.published.total",
            "service", serviceName).increment();
    }

    public void recordPublishFailed(String reason) {
        registry.counter("outbox.publish_failed.total",
            "service", serviceName,
            "reason", reason).increment();
    }

    public void recordPublishLatency(Duration latency) {
        registry.timer("outbox.publish.latency",
            "service", serviceName).record(latency);
    }

    // Gauge: events đang queue (chưa publish)
    @PostConstruct
    public void registerQueueGauge() {
        Gauge.builder("outbox.queue.size", outboxRepository,
            r -> r.countUnprocessed())
            .description("Number of outbox events waiting to publish")
            .register(registry);
    }
}
```

### 5.5. RED Method — Mọi service cần đo

| Letter | Meaning | Metric |
|---|---|---|
| **R** | Rate | requests per second |
| **E** | Errors | errors per second |
| **D** | Duration | latency distribution (P50, P95, P99) |

Auto-có sẵn cho HTTP:
```
http_server_requests_seconds_count       (R + E)
http_server_requests_seconds_bucket      (D - histogram)
```

PromQL query mẫu:
```promql
# Request rate per service (req/s)
sum(rate(http_server_requests_seconds_count[5m])) by (service)

# Error rate (%)
sum(rate(http_server_requests_seconds_count{status=~"5.."}[5m])) by (service)
  / 
sum(rate(http_server_requests_seconds_count[5m])) by (service)
  * 100

# P99 latency
histogram_quantile(0.99, 
  sum(rate(http_server_requests_seconds_bucket[5m])) by (le, service))
```

---

## 6. Grafana Dashboards

### 6.1. Setup Grafana datasource

Truy cập `http://localhost:3000` (admin/admin) → Connections → Add Prometheus:
- URL: `http://prometheus:9090`

### 6.2. Dashboard 1: Saga Health

**Panels cần có:**
1. **Saga Success Rate** (gauge, last 5m)
   ```promql
   sum(rate(saga_completed_total[5m])) /
   (sum(rate(saga_completed_total[5m])) + sum(rate(saga_failed_total[5m])))
   * 100
   ```

2. **Saga Throughput** (graph, req/s over time)
   ```promql
   sum(rate(saga_started_total[5m]))
   ```

3. **Active Sagas by State** (stacked bar)
   ```promql
   saga_active_count{state!~"COMPLETED|FAILED"}
   ```

4. **P50/P95/P99 Saga Duration**
   ```promql
   histogram_quantile(0.99, rate(saga_duration_seconds_bucket[5m]))
   ```

5. **Failure Breakdown by Reason** (pie chart)
   ```promql
   sum(increase(saga_failed_total[1h])) by (failure_reason)
   ```

### 6.3. Dashboard 2: Service Health (per service)

- HTTP request rate
- HTTP error rate
- HTTP P99 latency
- JVM heap usage
- DB connection pool active/idle
- Kafka consumer lag

### 6.4. Dashboard 3: Messaging Infrastructure

- **Outbox queue size** (warning at >100, critical >1000)
- **Outbox publish latency** (P99)
- **Outbox retry count distribution**
- **Inbox dedup rate** (% messages skipped as duplicate)
- **Kafka consumer lag** per topic/group
- **Kafka producer error rate**

### 6.5. Import/export dashboards

Save dashboard as JSON → commit vào repo: `infra/grafana/dashboards/saga-health.json`

→ Anyone setup local có thể import → cùng view.

---

## 7. Structured Logging

### 7.1. JSON logs thay vì plain text

`logback-spring.xml`:
```xml
<configuration>
    <springProfile name="!local">
        <appender name="JSON" class="ch.qos.logback.core.ConsoleAppender">
            <encoder class="net.logstash.logback.encoder.LogstashEncoder">
                <includeMdcKeyName>traceId</includeMdcKeyName>
                <includeMdcKeyName>spanId</includeMdcKeyName>
                <includeMdcKeyName>sagaId</includeMdcKeyName>
                <includeMdcKeyName>orderId</includeMdcKeyName>
                <customFields>{"service":"${spring.application.name}"}</customFields>
            </encoder>
        </appender>
        <root level="INFO">
            <appender-ref ref="JSON"/>
        </root>
    </springProfile>
</configuration>
```

→ Mỗi log line là JSON:
```json
{
  "@timestamp": "2026-06-16T10:30:00.123Z",
  "level": "INFO",
  "service": "saga-orchestrator",
  "traceId": "7b3e1a8d4f9c2e0a1234567890abcdef",
  "spanId": "a1b2c3d4e5f6g7h8",
  "sagaId": "abc-123",
  "orderId": "def-456",
  "message": "Saga transition: AWAITING_PAYMENT -> AWAITING_SHIPMENT"
}
```

→ Query với Elasticsearch / Loki:
```
service:"saga-orchestrator" AND sagaId:"abc-123"
```

### 7.2. MDC (Mapped Diagnostic Context)

```java
@Service
public class SagaOrchestrator {

    public void handleReply(SagaReply reply) {
        MDC.put("sagaId", reply.getSagaId());
        MDC.put("orderId", lookupOrderId(reply.getSagaId()).toString());
        try {
            // ... business logic
            log.info("Handling reply");   // log có MDC fields auto
        } finally {
            MDC.clear();
        }
    }
}
```

Hoặc dùng filter:
```java
@Component
public class MdcFilter extends OncePerRequestFilter {
    @Override
    protected void doFilterInternal(HttpServletRequest req, ...) {
        String requestId = req.getHeader("X-Request-Id");
        if (requestId == null) requestId = UUID.randomUUID().toString();
        MDC.put("requestId", requestId);
        try {
            filterChain.doFilter(req, resp);
        } finally {
            MDC.clear();
        }
    }
}
```

### 7.3. Log Aggregation (preview)

Production: dùng **Loki** (lightweight, dùng PromQL-like) hoặc **ELK** (powerful, expensive).

```
Service logs ─► Promtail/Fluentd ─► Loki ─► Grafana (cùng UI với metrics!)
```

Trong Grafana có thể query log + metric cùng dashboard.

→ Demo bỏ qua phần này, nhưng setup tương lai dễ.

---

## 8. Alerts với Prometheus Alertmanager

### 8.1. Alert categories

| Severity | Action | Response time |
|---|---|---|
| **Critical** | Page on-call immediately | < 5 phút |
| **Warning** | Ticket, fix trong day | < 24h |
| **Info** | Slack notify, no action needed | - |

### 8.2. Alert rules cho Saga

`infra/prometheus/alerts.yml`:
```yaml
groups:
  - name: saga
    interval: 30s
    rules:
      # Critical: tỉ lệ fail vượt 5%
      - alert: SagaHighFailureRate
        expr: |
          (sum(rate(saga_failed_total[5m])) 
           / 
           (sum(rate(saga_completed_total[5m])) + sum(rate(saga_failed_total[5m]))))
          > 0.05
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Saga failure rate > 5%"
          description: "Current: {{ $value | humanizePercentage }}"

      # Warning: outbox backlog
      - alert: OutboxBacklog
        expr: outbox_queue_size > 100
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "Outbox queue size {{ $value }} on {{ $labels.service }}"

      # Critical: outbox stuck
      - alert: OutboxStuck
        expr: outbox_queue_size > 1000
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Outbox severely backlogged on {{ $labels.service }}"

      # Critical: saga stuck (in non-terminal state > 10 phút)
      - alert: SagaStuck
        expr: |
          max_over_time(saga_active_count{state!~"COMPLETED|FAILED"}[10m]) > 0
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Sagas stuck in state {{ $labels.state }}"

      # Critical: P99 latency > 30s
      - alert: SagaP99LatencyHigh
        expr: |
          histogram_quantile(0.99, 
            rate(saga_duration_seconds_bucket{outcome="completed"}[5m])) > 30
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Saga P99 latency > 30s"

      # Critical: Kafka consumer lag
      - alert: KafkaConsumerLagHigh
        expr: kafka_consumergroup_lag > 1000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Consumer lag {{ $value }} on {{ $labels.group }}"
```

### 8.3. SLO-based alerting (advanced)

Định nghĩa SLO: "99.5% sagas hoàn thành trong 30 giây".

**Error budget**: 0.5% sagas có thể fail/slow trong 1 tháng.

**Burn rate alert**: nếu error budget cháy quá nhanh → alert.

```yaml
- alert: SagaSloFastBurn
  expr: |
    (1 - 
      (sum(rate(saga_completed_total[1h])) 
       / 
       sum(rate(saga_started_total[1h]))))
    > (1 - 0.995) * 14.4    # 14.4x = burn rate fast (2% budget in 1h)
  for: 5m
  labels:
    severity: critical
```

→ Cao cấp, áp dụng cho production team có ngân sách quan sát rõ ràng.

### 8.4. Alert fatigue — Tránh

- Quá nhiều alert → team ignore → critical alert miss
- Mỗi alert phải có **runbook**: "khi alert này fire, làm gì?"
- Alert nào fire > 5 lần/tuần → review (raise threshold, fix root cause, hoặc delete)

---

## 9. Use case thực tế: Debug 1 saga fail

Đây là kịch bản mô phỏng:

**Setup**: Saga fail rate đột nhiên tăng → alert fires.

### Step 1: Open Grafana dashboard
Saga Health → thấy:
- Failure rate jump từ 1% → 15%
- Failure reason chính: "Payment Gateway Timeout"

### Step 2: Identify time range
Failures start 10:30 AM → có deploy nào lúc đó? Có spike traffic?

### Step 3: Drill down vào 1 failed saga
Pick 1 sagaId từ logs:
```bash
docker logs saga-orchestrator | grep "failed" | head -5
# → sagaId=abc-123
```

### Step 4: Tìm trace
Jaeger UI → search `saga.id=abc-123` → mở waterfall:
- Span Order Service: 200ms ✓
- Span Saga: 50ms ✓
- Span Inventory: 100ms ✓
- Span Payment: **30,000ms** ← bottleneck
- Inside Payment span: HTTP call to gateway: 30s timeout

### Step 5: Verify hypothesis
Check payment_db:
```sql
SELECT count(*), avg(EXTRACT(EPOCH FROM (updated_at - created_at)))
FROM payments
WHERE created_at > now() - interval '30 min';
```
→ Avg duration tăng từ 200ms lên 5s → confirm gateway slow.

### Step 6: Contact gateway provider
Show họ timestamps, request IDs.

### Step 7: Mitigation
- Tăng timeout? Không (chỉ delay vấn đề)
- Circuit breaker? Có (sẽ học bổ sung)
- Switch to fallback gateway? Implement.

**Tổng thời gian debug: ~10 phút.** Trước observability: vài giờ.

---

## 10. Configuration cho Production vs Dev

### Dev (sampling 100%)
```yaml
management:
  tracing:
    sampling:
      probability: 1.0      # trace mọi request
```
→ Mọi request → trace → debug dễ. Nhưng overhead cao.

### Production (sampling 1-10%)
```yaml
management:
  tracing:
    sampling:
      probability: 0.01     # trace 1% requests
```
→ Giảm storage, giảm CPU overhead. Vẫn đủ thấy trend.

### Tail-based sampling (advanced)
- Đợi đến cuối trace mới quyết định keep/drop
- Keep 100% errors + slow requests
- Drop 99% normal traces
- Cần OpenTelemetry Collector

### Sampling theo endpoint
```yaml
sampling:
  rules:
    - service: payment-service
      probability: 0.1     # 10% payment (vì quan trọng)
    - service: order-service  
      probability: 0.01    # 1% order (high volume)
```

---

## 11. Bẫy phổ biến

### Bẫy 1: Trace ID không propagate qua Kafka
Manual produce Kafka message → quên copy `traceparent` header → trace bị đứt.
→ Dùng Spring Kafka chuẩn (đã handle), không dùng raw producer.

### Bẫy 2: Sampling 100% trong production
Storage explosion: 1 request = 50 spans, 10k req/s = 500k spans/s.
→ Dev 100%, prod 1-10%.

### Bẫy 3: Span attribute chứa PII
`span.setAttribute("user.email", email)` → leak data sang Jaeger.
→ Hash hoặc redact PII trước khi attach.

### Bẫy 4: Quá nhiều custom span
1 method = 1 span = quá detail. Mỗi span có overhead.
→ Span ở boundary (HTTP, Kafka, DB), không phải mỗi method.

### Bẫy 5: Metrics cardinality explosion
```java
registry.counter("requests", "user_id", userId).increment();
```
→ 1 triệu user = 1 triệu time series = Prometheus chết.
→ Không bao giờ dùng high-cardinality field (user_id, order_id) làm label.

### Bẫy 6: Alert mà không có runbook
"SagaHighFailureRate fires!" → on-call không biết làm gì.
→ Mỗi alert: link runbook "khi alert này fire, làm 5 bước này".

### Bẫy 7: Threshold tùy tiện
"Alert khi latency > 1s" — vì sao 1s? Phải dựa vào baseline + SLO.

### Bẫy 8: Log mọi thứ ở INFO
INFO mà log mọi DB query → log volume khổng lồ, chậm, đắt.
→ DEBUG cho chi tiết, INFO cho business event.

### Bẫy 9: Quên correlation trong async
Khi message vào Kafka, consumer xử lý ở thread khác → MDC bị mất.
→ Inject traceId từ Kafka header vào MDC trong listener.

### Bẫy 10: Coi observability là afterthought
Build xong mới thêm log/metric → vá víu, miss nhiều thứ.
→ **Instrument từ đầu**, build observability vào kiến trúc.

---

## 12. Demo show off

### Sau bài 13, demo của bạn có:

1. **Click 1 nút "Generate Traffic"** (script bắn 100 orders/min)
2. **Mở Grafana dashboard** → real-time graph
3. **Trigger chaos** (kill 1 service)
4. **Watch dashboard react**:
   - Service health drop
   - Saga stuck count tăng
   - Outbox lag tăng
5. **Open Jaeger** → trace 1 stuck saga → see exactly where it stuck
6. **Restart service** → watch recovery in real-time
7. **Show alert fired + resolved** trong Alertmanager

→ **Đây là demo level "Solution Architect"** — không chỉ code chạy, mà **operate được**.

---

## 13. Gợi ý thực hành

1. Setup Jaeger + Prometheus + Grafana trong docker-compose (đã có ở bài 7).
2. Add tracing dependencies cho 5 service, restart, verify traces xuất hiện trong Jaeger.
3. Tạo 1 order → mở Jaeger → show toàn flow.
4. Thêm 5 custom Saga metrics, tạo Grafana dashboard "Saga Health".
5. Bắn 1000 orders → quan sát dashboard real-time.
6. Inject chaos (kill payment-service) → quan sát alert fire.
7. Tạo runbook cho 5 alert chính: SagaHighFailureRate, OutboxBacklog, SagaStuck, KafkaConsumerLagHigh, ServiceDown.
8. Export Grafana dashboard JSON → commit repo.
9. Practice "debug story" với mentor: họ cho 1 sagaId fail, bạn dùng tracing/metrics/logs tìm root cause trong 5 phút.

---

## Checklist tự đánh giá

- [ ] Phân biệt 3 trụ cột: Logs / Metrics / Traces, khi nào dùng cái nào
- [ ] Setup OpenTelemetry trong Spring Boot
- [ ] Verify trace propagate qua HTTP và Kafka
- [ ] Thêm custom span cho Saga state transitions
- [ ] Query Jaeger UI theo tag, latency, error
- [ ] Hiểu RED method và USE method
- [ ] Implement custom Saga metrics (started/completed/failed/duration)
- [ ] Cấu hình Prometheus scrape, Grafana datasource
- [ ] Build 3 dashboards (Saga Health / Service Health / Messaging Infra)
- [ ] Structured logging với MDC + correlation ID
- [ ] Setup 5+ alerts với threshold hợp lý
- [ ] Có runbook cho mỗi alert
- [ ] Hiểu sampling strategy (dev vs prod)
- [ ] Liệt kê 10 bẫy observability
- [ ] Practice debug 1 saga fail dùng 3 trụ cột

---

**Tổng kết phần core (Bài 1-13):**

Bạn đã có:
- ✅ Lý thuyết nền tảng (ACID → Saga → Outbox → Idempotency)
- ✅ System chạy được end-to-end (5 services + Orchestrator)
- ✅ Testing tự động (Unit + Integration + E2E)
- ✅ Failure injection (chaos engineering)
- ✅ Observability (3 pillars)

→ **Đây là bộ tools đầy đủ của Senior Backend Engineer.**

**Phần Bonus tới (tùy chọn):**
- Bài 14: Debezium CDC (migrate Polling → Log-tailing)
- Bài 15: Choreography variant để so sánh
- Bài 16: Interview prep + tổng kết

Nói "tiếp" để vào phần bonus, hoặc dừng tại đây và **đi thực hành** với hệ thống đã build.
