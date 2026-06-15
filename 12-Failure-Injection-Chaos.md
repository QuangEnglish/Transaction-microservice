# Failure Injection & Chaos Engineering nhẹ

> Bài 12 trong lộ trình Transaction Microservice nâng cao.
> Mục tiêu: Chủ động phá hệ thống — kill service, kill Kafka, kill DB, inject latency — để **xác minh** kiến trúc thực sự bền vững. Đây là phần "chứng minh" tất cả pattern đã build có giá trị, không chỉ trên giấy.

---

## 0. Tại sao Failure Injection quan trọng

> "Untested code is broken code. Untested failure paths are broken failure paths."

Mọi pattern bạn đã build — Outbox, Inbox, Saga, Idempotency, Timeout — đều design cho **failure scenarios**. Nhưng nếu không **test failure scenarios**, bạn không biết chúng có thực sự work hay không.

Câu phỏng vấn classic:
> *"Nếu Kafka down 5 phút giữa lúc Saga đang chạy, hệ thống của bạn sẽ thế nào?"*

Trả lời "Outbox sẽ buffer events, khi Kafka up sẽ publish" — **OK**.
Trả lời "Tôi đã test scenario đó: kill Kafka 5 phút, restart, verify 0 message mất" — **xuất sắc**.

→ Bài này dạy bạn trả lời cách thứ 2.

---

## 1. Chaos Engineering — Nguyên tắc

Netflix định nghĩa **Chaos Engineering** = "Khoa học của việc thí nghiệm trên hệ thống phân tán để xây dựng niềm tin vào khả năng chịu tải của nó trong điều kiện turbulent (hỗn loạn)."

### 5 nguyên tắc cốt lõi

1. **Xây hypothesis trước**: "Tôi tin rằng nếu Kafka down 2 phút, sẽ không có message mất". Test để verify hoặc bác bỏ.
2. **Vary real-world events**: server crashes, network partitions, latency spikes — không phải các scenario "lý tưởng hóa".
3. **Run experiments in production** (cuối cùng): test trong staging trước, production game day sau khi đã tin.
4. **Automate**: failure injection tự động, định kỳ, không phải one-shot.
5. **Minimize blast radius**: test với 1 instance, 1 user, 1 partition trước khi scale up.

### Maturity ladder

```
Level 0: Không test failure (đa số dev/team)
Level 1: Manual failure injection trong dev (bài này)
Level 2: Automated failure tests trong CI
Level 3: Scheduled chaos in staging
Level 4: Production game days
Level 5: Continuous chaos in production (Netflix-level)
```

→ Mục tiêu bài này: đạt **Level 1-2** cho demo. Level 3+ là cho enterprise team có on-call rotation.

---

## 2. Categories of Failures

| Loại | Ví dụ | Pattern bảo vệ |
|---|---|---|
| **Process crash** | Service bị OOM kill, restart | Stateless service, persist state ở DB |
| **Database failure** | DB down, connection pool cạn, slow query | Connection pool, retry, circuit breaker |
| **Message broker** | Kafka broker chết, partition leader fail | Outbox buffer, replication |
| **Network** | Packet loss, high latency, partition | Timeout, retry với backoff, idempotency |
| **External API** | Payment gateway timeout / 500 | Circuit breaker, fallback, async |
| **Resource exhaustion** | Disk full, memory leak, thread pool cạn | Limits, alerting, graceful degradation |
| **Time anomaly** | Clock skew, NTP drift | Use logical time, không trust wall clock |
| **Data corruption** | Bug serialize, schema mismatch | Schema validation, DLQ |

→ Bài này focus 4 loại đầu (process, DB, broker, network) vì liên quan trực tiếp Saga.

---

## 3. Tools

### 3.1. Docker commands (basic)

```bash
# Kill 1 container (graceful shutdown)
docker stop <container>

# Kill mạnh (SIGKILL)
docker kill <container>

# Pause container (đóng băng, không xóa)
docker pause <container>
docker unpause <container>

# Restart
docker restart <container>

# Disconnect khỏi network
docker network disconnect <network> <container>
docker network connect <network> <container>
```

### 3.2. Toxiproxy — Network chaos

Network proxy programmable, inject latency/loss/disconnect giữa client và service.

```bash
docker run -d --name toxiproxy \
  -p 8474:8474 -p 8666:8666 \
  ghcr.io/shopify/toxiproxy
```

Tạo proxy giữa app và Kafka:
```bash
# Proxy traffic to kafka qua port 8666
curl -X POST http://localhost:8474/proxies \
  -d '{"name":"kafka","listen":"0.0.0.0:8666","upstream":"kafka:29092"}'

# Inject latency 2s
curl -X POST http://localhost:8474/proxies/kafka/toxics \
  -d '{"type":"latency","attributes":{"latency":2000}}'

# Inject 30% packet loss
curl -X POST http://localhost:8474/proxies/kafka/toxics \
  -d '{"type":"limit_data","attributes":{"bytes":1024}}'

# Remove toxic
curl -X DELETE http://localhost:8474/proxies/kafka/toxics/<toxic-name>
```

Cấu hình app trỏ tới Toxiproxy thay vì Kafka trực tiếp:
```yaml
spring.kafka.bootstrap-servers: localhost:8666
```

### 3.3. Pumba — Docker chaos tool

```bash
# Random kill 1 container mỗi 30s từ pattern
pumba --random --interval 30s kill "re2:saga-.*"

# Inject network delay 1000ms ±500ms vào kafka
pumba netem --duration 2m --tc-image gaiadocker/iproute2 \
  delay --time 1000 --jitter 500 kafka

# Stop container random
pumba --random pause --duration 30s "re2:saga-.*"
```

### 3.4. Spring Boot — Graceful shutdown

```yaml
server:
  shutdown: graceful
spring:
  lifecycle:
    timeout-per-shutdown-phase: 30s
```

→ Khi `docker stop` (SIGTERM): Spring đợi in-flight requests xong rồi mới shutdown. Khác `docker kill` (SIGKILL): kill ngay.

→ Production luôn dùng graceful shutdown.

### 3.5. Failure switches trong code

Endpoint actuator-style để toggle failure cho test:
```java
@RestController
@RequestMapping("/test/chaos")
@Profile("chaos")  // chỉ active khi profile bật
public class ChaosController {

    @Setter
    private double paymentFailureRate = 0.0;
    @Setter
    private long paymentLatencyMs = 0;
    @Setter
    private boolean stockReservationDisabled = false;

    @PostMapping("/payment-failure-rate")
    public void setPaymentFailureRate(@RequestParam double rate) {
        this.paymentFailureRate = rate;
    }
    // ... các setter khác
}
```

→ Inject failure mà không restart service.

---

## 4. Scenario 1: Kill Saga Orchestrator giữa flow

### Hypothesis
> Saga state đã persisted trong DB. Khi orchestrator crash giữa flow, restart sẽ resume đúng từ state đó, không có data loss hay double processing.

### Setup
```bash
docker compose up -d
mvn spring-boot:run -pl saga-orchestrator &
ORCHESTRATOR_PID=$!
# ... start các service khác

# Seed inventory
docker exec postgres psql -U admin -d inventory_db -c "
  INSERT INTO inventory(product_id, available_quantity) VALUES ('product-A', 100);"
```

### Inject failure
```bash
# 1. Tạo order → Saga bắt đầu
ORDER_RESPONSE=$(curl -X POST http://localhost:8081/api/orders -d '{...}')
ORDER_ID=$(echo $ORDER_RESPONSE | jq -r .id)

# 2. Đợi 500ms để Saga bắt đầu (ở AWAITING_PAYMENT chẳng hạn)
sleep 0.5

# 3. KILL orchestrator (no graceful)
kill -9 $ORCHESTRATOR_PID

# 4. Quan sát: order vẫn PENDING, các service khác vẫn chạy
curl http://localhost:8081/api/orders/$ORDER_ID
# → status: PENDING

# 5. Khi reply về từ Payment service, message sẽ queue trong Kafka (chưa ai consume)

# 6. Restart orchestrator
mvn spring-boot:run -pl saga-orchestrator &
ORCHESTRATOR_PID=$!

# 7. Đợi 5-10s → verify order CONFIRMED
sleep 10
curl http://localhost:8081/api/orders/$ORDER_ID
# → status: CONFIRMED (saga đã resume)
```

### Verify
```sql
-- saga_db
SELECT id, current_state, started_at, completed_at FROM saga_instances WHERE order_id = ?;
-- → state=COMPLETED, completed_at != null
SELECT step_no, step_type, command_type FROM saga_steps WHERE saga_id = ? ORDER BY step_no;
-- → đầy đủ các step, không có gap

-- inventory_db
SELECT * FROM stock_reservations WHERE order_id = ?;
-- → status=COMMITTED

-- payment_db
SELECT * FROM payments WHERE order_id = ?;
-- → status=COMPLETED

-- order_db
SELECT status, version FROM orders WHERE id = ?;
-- → status=CONFIRMED, version đúng (không bị double update)
```

### Hypothesis verified
- ✅ Saga state persisted → resume được
- ✅ Kafka consumer offset → đọc lại replies missed
- ✅ Inbox/Idempotency → không double process

→ **Nếu test này pass, bạn đã chứng minh kiến trúc Saga đúng.**

---

## 5. Scenario 2: Kill Kafka giữa lúc Outbox đang publish

### Hypothesis
> Khi Kafka down, Outbox events vẫn được persist trong DB. Khi Kafka up trở lại, OutboxRelay sẽ publish các events đã backlog.

### Inject
```bash
# 1. Kill Kafka container
docker stop kafka

# 2. Trong 30s Kafka down, tạo 10 orders
for i in {1..10}; do
  curl -X POST http://localhost:8081/api/orders -d '{"customerId":"c'$i'","items":[...]}'
done

# 3. Verify: orders đã save trong DB, outbox đã save, nhưng processed_at vẫn NULL
docker exec postgres psql -U admin -d order_db -c "
  SELECT count(*) FROM orders;                            -- 10
  SELECT count(*) FROM outbox_events;                     -- 10
  SELECT count(*) FROM outbox_events WHERE processed_at IS NULL;  -- 10 (chưa publish)
"

# 4. Verify OutboxRelay đang log lỗi (retry mỗi 500ms)
docker logs order-service | tail -50
# → "Failed to publish event ..." (đúng, vì Kafka down)

# 5. Restart Kafka
docker start kafka
sleep 30  # đợi Kafka stable

# 6. Verify outbox events đã publish hết (sau 1-2 phút)
docker exec postgres psql -U admin -d order_db -c "
  SELECT count(*) FROM outbox_events WHERE processed_at IS NULL;  -- 0
  SELECT max(retry_count) FROM outbox_events;             -- > 0 (đã retry)
"

# 7. Verify các order tiếp tục flow tới CONFIRMED
sleep 30
for orderId in ...; do
  curl http://localhost:8081/api/orders/$orderId | jq .status
done
# → tất cả CONFIRMED
```

### Verify hypothesis
- ✅ Order save vẫn thành công khi Kafka down
- ✅ Outbox events queue up trong DB
- ✅ OutboxRelay retry với backoff (`retry_count` tăng)
- ✅ Khi Kafka up → publish hết, đúng order
- ✅ Flow downstream tiếp tục không có dấu hiệu Kafka đã từng down

### Variant: Kafka down 30 phút (long outage)

Quan sát:
- `outbox_events.retry_count` tăng tới max retries (config 5)
- Events có `retry_count >= 5` sẽ bị skip → vào "stuck" state
- Cần alert + manual intervention (admin endpoint reset retry_count)

→ Production cần monitoring `outbox_events.retry_count > threshold` → alert.

---

## 6. Scenario 3: DB connection failure giữa transaction

### Hypothesis
> Nếu DB connection fail giữa transaction, transaction rollback → không có dữ liệu nửa vời.

### Inject (PostgreSQL approach)

```bash
# 1. Drop connection của 1 transaction đang chạy
# (Cần biết PID trong PostgreSQL của connection đó)

docker exec postgres psql -U admin -d order_db -c "
  SELECT pid, query, state FROM pg_stat_activity 
  WHERE datname='order_db' AND state != 'idle';"

# 2. Force terminate
docker exec postgres psql -U admin -c "SELECT pg_terminate_backend(<pid>);"
```

### Alternative: Toxiproxy giữa app và DB

```bash
# Setup Toxiproxy
curl -X POST http://localhost:8474/proxies \
  -d '{"name":"postgres","listen":"0.0.0.0:5433","upstream":"postgres:5432"}'

# App trỏ tới port 5433 thay vì 5432
# spring.datasource.url: jdbc:postgresql://localhost:5433/order_db

# Inject 5s latency
curl -X POST http://localhost:8474/proxies/postgres/toxics \
  -d '{"type":"latency","attributes":{"latency":5000}}'

# Hoặc disconnect tất cả
curl -X POST http://localhost:8474/proxies/postgres/toxics \
  -d '{"type":"timeout","attributes":{"timeout":0}}'
```

### Verify
```bash
# Tạo order trong lúc DB latency cao
curl -X POST http://localhost:8081/api/orders -d '{...}'

# Quan sát logs:
# - Có thể fail với timeout → 500 response (đúng)
# - Hoặc thành công nhưng chậm

# Verify atomicity:
docker exec postgres psql -U admin -d order_db -c "
  SELECT 
    (SELECT count(*) FROM orders) AS orders,
    (SELECT count(*) FROM outbox_events) AS outbox;"

# → Hai số phải bằng nhau (không có order without outbox, không có outbox without order)
```

### Critical assertion

> **Số lượng `orders` phải BẰNG số lượng `outbox_events`.** Mọi sai khác = có dual write somewhere.

---

## 7. Scenario 4: Duplicate messages (force redeliver)

### Hypothesis  
> Idempotent consumer xử lý đúng 1 lần dù message đến nhiều lần.

### Inject
```bash
# Cách 1: Replay từ offset
# Reset consumer group offset về earliest → tất cả messages được consume lại
docker exec kafka kafka-consumer-groups \
  --bootstrap-server kafka:29092 \
  --group inventory-service \
  --reset-offsets --to-earliest \
  --topic saga.commands.inventory \
  --execute

# Cách 2: Manual produce duplicate
# Sample 1 message từ existing topic, produce lại với cùng commandId
```

### Verify
```bash
# Trước reset
docker exec postgres psql -U admin -d inventory_db -c "
  SELECT count(*) FROM stock_reservations;        -- ví dụ: 10
  SELECT count(*) FROM inbox_events;              -- ví dụ: 10
"

# Reset offset → consumer xử lý lại tất cả messages
# Đợi 30s

# Sau khi consume lại:
docker exec postgres psql -U admin -d inventory_db -c "
  SELECT count(*) FROM stock_reservations;        -- VẪN 10 (idempotent!)
  SELECT count(*) FROM inbox_events;              -- VẪN 10 (UNIQUE constraint chặn)
  
  -- Verify available quantity không bị trừ thêm
  SELECT * FROM inventory;
"
```

### Verify hypothesis
- ✅ Duplicate messages không gây duplicate reservation
- ✅ Inventory không bị trừ thêm
- ✅ Inbox UNIQUE constraint bảo vệ DB level

→ **Pass = Idempotency thực sự work.**

---

## 8. Scenario 5: Service crash sau khi process, trước ack

### Hypothesis
> Nếu service xử lý xong nhưng crash trước khi ack Kafka, message redeliver. Idempotency check → skip, không double effect.

### Inject (Khó vì cần timing chính xác)

**Cách 1: Code trick** — thêm crash point trong @Profile chaos:
```java
@Component
@Profile("chaos")
public class CrashPoint {
    
    @Value("${chaos.crash-after-process:false}")
    private boolean crashAfterProcess;
    
    public void maybeCrash() {
        if (crashAfterProcess) {
            log.error("CHAOS: forcing crash");
            System.exit(1);
        }
    }
}

// Trong SagaCommandHandler:
@Transactional
public void handleReserveStock(...) {
    idempotentConsumer.processOnce(...);
    crashPoint.maybeCrash();  // crash sau khi commit, trước khi ack Kafka
}
```

**Cách 2: SIGKILL kịp thời**
```bash
# Trigger flow, theo dõi log
docker logs -f inventory-service | grep "Stock reserved"

# Khi thấy "Stock reserved": SIGKILL ngay
kill -9 $(pgrep -f inventory-service)
```

### Verify
```bash
# DB đã có data
docker exec postgres psql -U admin -d inventory_db -c "
  SELECT * FROM stock_reservations WHERE order_id = ?;        -- có
  SELECT * FROM inbox_events WHERE event_id = ?;              -- có
"

# Kafka offset chưa commit (vì process bị kill trước ack)
docker exec kafka kafka-consumer-groups \
  --bootstrap-server kafka:29092 \
  --describe --group inventory-service

# → offset < end-offset → có message chưa consume

# Restart service
mvn spring-boot:run -pl inventory-service &

# Service consume lại message → vào idempotency check → skip
docker logs inventory-service | grep "already processed"

# Verify: KHÔNG có reservation thêm, KHÔNG có inventory thay đổi
docker exec postgres psql -U admin -d inventory_db -c "
  SELECT count(*) FROM stock_reservations WHERE order_id = ?;     -- VẪN 1
  SELECT available_quantity FROM inventory WHERE product_id = ?;  -- KHÔNG đổi
"
```

---

## 9. Scenario 6: Network partition giữa Saga ↔ Inventory

### Hypothesis
> Nếu network partition kéo dài, Saga timeout → trigger compensation. Khi partition heal, các command compensation chạy bình thường (idempotent).

### Inject
```bash
# Disconnect inventory-service khỏi kafka network
docker network disconnect saga-demo_default inventory-service

# Hoặc dùng Toxiproxy: block 100% traffic
curl -X POST http://localhost:8474/proxies/kafka-for-inventory/toxics \
  -d '{"type":"timeout","attributes":{"timeout":0}}'

# Bây giờ Saga gửi ReserveStock → Inventory không nhận được

# Đợi 2 phút (= STEP_TIMEOUT trong SagaTimeoutWatcher)
sleep 130

# Saga timeout watcher fire → tạo synthetic fail reply → trigger compensation
# Vì stock chưa reserve được nên không có gì để release
# → CancelOrder command → order CANCELLED

# Verify
curl http://localhost:8081/api/orders/$ORDER_ID
# → status: CANCELLED
curl http://localhost:8085/api/sagas/by-order/$ORDER_ID
# → state: FAILED, reason: "Timeout waiting for AWAITING_STOCK_RESERVATION"

# Heal partition
docker network connect saga-demo_default inventory-service

# Verify: inventory-service eventually consume command (cũ), check idempotency, skip
docker logs inventory-service | grep "already processed\|stock reserve"
```

### Variant: Partition giữa flow

Bắt đầu Saga, để stock reserve xong, payment xong, rồi partition Shipping:
- Saga ở AWAITING_SHIPMENT
- Timeout → fail → compensation chain
- Payment refund OK
- Stock release OK
- Order CANCELLED

Khi heal: shipment service consume command cũ → idempotent skip.

---

## 10. Scenario 7: Slow consumer (back pressure)

### Hypothesis
> Nếu 1 consumer xử lý chậm (DB slow, GC pause), lag tăng. Hệ thống không crash, chỉ chậm hơn. Eventual consistency được duy trì.

### Inject
```bash
# Inject latency vào DB của inventory-service
# (Toxiproxy giữa inventory-service và postgres, latency 2000ms)

# Hoặc inject latency code:
# Trong InventoryService.reserve(), thêm Thread.sleep(2000)

# Bắn 100 orders cùng lúc
for i in {1..100}; do
  curl -X POST http://localhost:8081/api/orders -d '{...}' &
done
wait

# Quan sát Kafka consumer lag
docker exec kafka kafka-consumer-groups \
  --bootstrap-server kafka:29092 \
  --describe --group inventory-service

# Lag tăng (vài chục/trăm), nhưng không crash
```

### Verify
- ✅ Hệ thống không crash dù chậm
- ✅ Sau khi remove latency, lag giảm về 0
- ✅ Tất cả 100 order eventually → CONFIRMED
- ✅ Không có message mất

### Monitor metrics liên quan
- `kafka_consumer_lag` per consumer group
- `outbox_unprocessed_count` per service
- `saga_active_count` per state

→ Alert khi các metric này vượt threshold.

---

## 11. Scenario 8: Out-of-order messages

### Hypothesis
> Cùng partition Kafka → order preserved. Nhưng nếu cross-partition, có thể out-of-order. Saga state machine reject invalid transitions.

### Inject (khó)

Out-of-order trong cùng partition là hiếm (Kafka đảm bảo). Test bằng cách produce manual:

```bash
# Produce PaymentCompleted reply TRƯỚC khi StockReserved reply
# (giả lập out-of-order)

docker exec -i kafka kafka-console-producer \
  --bootstrap-server kafka:29092 \
  --topic saga.replies \
  --property "headers=event-type:PaymentCommandReply" <<EOF
{"replyId":"...","commandId":"...","sagaId":"$SAGA_ID","success":true}
EOF

# Lúc này saga đang ở AWAITING_STOCK_RESERVATION
# → receive PaymentCommandReply → determineNextState() throw IllegalStateException
```

### Verify
- Orchestrator log error nhưng không crash
- Saga giữ nguyên state, không transition sai
- Message vào DLQ sau N retries
- Alert ops investigate

→ **State machine validate được catch các transition sai.**

→ Phòng tránh in-production: partition theo `sagaId` (đã làm).

---

## 12. Scenario 9: Disk full (resource exhaustion)

### Hypothesis
> Khi disk full, DB write fail → transaction rollback → flow fail an toàn, không có dữ liệu nửa vời.

### Inject
```bash
# Fill disk
docker exec postgres bash -c "dd if=/dev/zero of=/var/lib/postgresql/data/fill bs=1M count=10000"

# Tạo order → DB INSERT fail
curl -X POST http://localhost:8081/api/orders -d '{...}'
# → 500 Internal Server Error

# Cleanup
docker exec postgres rm /var/lib/postgresql/data/fill
```

### Verify
- API trả 500 (đúng — không thể đảm bảo, phải báo lỗi)
- Không có partial data: `orders` count == `outbox_events` count
- Client retry → khi disk OK → success

---

## 13. Implementing chaos endpoints

Cho phép trigger chaos dễ dàng trong staging/dev:

```java
@RestController
@RequestMapping("/internal/chaos")
@RequiredArgsConstructor
@Profile({"dev", "chaos"})
public class ChaosController {

    private final ChaosState chaosState;
    
    @PostMapping("/crash")
    public void crashNow() {
        log.error("CHAOS: crashing in 100ms");
        new Thread(() -> {
            try { Thread.sleep(100); } catch (Exception e) {}
            System.exit(137);  // SIGKILL exit code
        }).start();
    }

    @PostMapping("/db-latency")
    public void setDbLatency(@RequestParam long ms) {
        chaosState.setDbLatencyMs(ms);
    }

    @PostMapping("/kafka-latency")
    public void setKafkaLatency(@RequestParam long ms) {
        chaosState.setKafkaLatencyMs(ms);
    }

    @PostMapping("/payment-failure-rate")
    public void setPaymentFailureRate(@RequestParam double rate) {
        chaosState.setPaymentFailureRate(rate);
    }

    @PostMapping("/reset")
    public void reset() {
        chaosState.reset();
    }

    @GetMapping("/state")
    public ChaosState getState() {
        return chaosState;
    }
}
```

→ **Hoán đổi giá trị** trong runtime mà không restart service. Pattern này gọi là **feature flag for chaos**.

---

## 14. Game Day — Mẫu kịch bản

Game Day = buổi chaos experiment có chủ đích, có người quan sát, có hypothesis.

### Mẫu Game Day script

```
== Game Day: 2026-06-20 ==
Hypothesis: Khi Kafka broker primary down, hệ thống vẫn xử lý orders với độ latency tăng nhẹ.

Time: 14:00 - 16:00
Owner: Backend team
Observers: SRE, Product

Pre-flight checklist:
[ ] Grafana dashboards ready
[ ] Alerts paused (avoid noise)
[ ] On-call notified
[ ] Rollback plan ready: docker start kafka-1

Steps:
14:00 - Baseline: bắn 100 orders, đo P99 latency
14:15 - Stop kafka-1 broker
14:20 - Observe: 
        - Outbox events queueing
        - Consumer reconnect to other broker
        - End-to-end latency
14:30 - Bắn 100 orders trong điều kiện degraded
14:45 - Restart kafka-1
14:50 - Observe recovery
15:00 - Verify final state: 100% orders CONFIRMED, 0 data loss

Results:
- Hypothesis: VERIFIED / REJECTED
- Findings: ...
- Action items:
  - [ ] Increase outbox poll interval khi detect Kafka issue
  - [ ] Add alert ...
```

→ Mỗi Game Day tạo ra **action items cụ thể** → cải thiện hệ thống.

---

## 15. Automation — Chaos trong CI

Schedule chaos test hàng đêm:
```yaml
# .github/workflows/chaos.yml
name: Nightly Chaos Tests
on:
  schedule:
    - cron: '0 2 * * *'  # 2 AM mỗi đêm

jobs:
  chaos-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run chaos scenarios
        run: |
          ./scripts/start-stack.sh
          ./scripts/chaos/scenario-1-kill-orchestrator.sh
          ./scripts/chaos/scenario-2-kafka-down.sh
          ./scripts/chaos/scenario-3-network-partition.sh
          ./scripts/verify-no-data-loss.sh
```

→ Nếu chaos test fail → ngăn release, alert team.

---

## 16. Bẫy phổ biến

### Bẫy 1: Test chaos với load = 0
Chaos không có user traffic = vô nghĩa. Phải bắn load thực sự khi inject failure.

### Bẫy 2: Không có baseline
Phải đo metrics **trước** khi inject để biết cái gì là "bất thường". Không có baseline = không biết hệ thống đã degrade hay chưa.

### Bẫy 3: Không có "abort button"
Chaos quá tay → production xuống. Phải có script reset/rollback chạy 1-click.

### Bẫy 4: Inject quá nhiều cùng lúc
"Kill DB + Kafka + 2 service" → quá nhiều variable → không biết nguyên nhân fail nào. Test 1 thứ tại 1 thời điểm.

### Bẫy 5: Test trong dev mà confidence prod
Dev khác prod (resource, load, data scale). Staging gần prod nhất.

### Bẫy 6: Không log đủ
Inject chaos rồi không biết hệ thống làm gì. Phải có **observability trước chaos** (bài 13).

### Bẫy 7: Quên unhappy endings
Test "hệ thống recover thành công" → easy. Quên test "hệ thống fail an toàn" (no data corruption khi không recover được).

### Bẫy 8: Skip cleanup
Test 1 → inject latency Toxiproxy → quên remove → Test 2 chạy với latency vẫn còn → kết quả sai.
→ `@AfterEach` reset state.

### Bẫy 9: Coi chaos là one-shot
"Test xong rồi, không cần test nữa." → Mỗi release thay đổi → có thể break recovery. Phải automate + chạy regular.

### Bẫy 10: Confuse Chaos với QA
Chaos KHÔNG phải tìm bug functional. Chaos test **failure recovery + behavior under degradation**. Bug logic là việc của QA.

---

## 17. Gợi ý thực hành

1. Implement 9 scenarios trong bài này thành script bash. Run tự động.
2. Setup Toxiproxy giữa app ↔ Kafka, giữa app ↔ DB. Inject 5 toxic combinations.
3. Game Day với người khác (đồng nghiệp, mentor): họ design scenario, bạn run, cùng quan sát.
4. Đo "Recovery Time": từ lúc failure → lúc hệ thống stable lại. Set SLA: < 1 phút cho Kafka outage, < 30s cho service crash.
5. Đọc **"Chaos Engineering" book** của Casey Rosenthal & Nora Jones (O'Reilly).
6. Tìm hiểu **Chaos Mesh** (Kubernetes-native chaos tool).
7. Xem talks của Netflix về Chaos Monkey trên YouTube.

---

## Checklist tự đánh giá

- [ ] Hiểu 5 nguyên tắc Chaos Engineering
- [ ] Phân biệt levels of chaos maturity (0-5)
- [ ] Liệt kê 7+ loại failure cần test
- [ ] Setup Toxiproxy, inject latency/loss
- [ ] Run scenario kill saga-orchestrator → verify resume
- [ ] Run scenario kill Kafka 1-5 phút → verify outbox queue + replay
- [ ] Run scenario duplicate messages → verify idempotency
- [ ] Run scenario network partition → verify timeout + compensation
- [ ] Run scenario slow consumer → verify back pressure handling
- [ ] Critical assertion: `orders.count == outbox.count` always
- [ ] Có chaos endpoint (`/internal/chaos/*`) để toggle runtime
- [ ] Hiểu Game Day là gì, viết được template
- [ ] Liệt kê 10 bẫy chaos engineering và cách tránh

---

**Bài tiếp theo:** `13 - Observability: Distributed Tracing, Metrics & Alerts` — sau khi biết phá hệ thống, học cách **quan sát** nó. Không có observability = chaos engineering bị mù.
