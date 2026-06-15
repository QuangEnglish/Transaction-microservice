# Outbox Pattern: Giải quyết Dual Write Problem

> Bài 5 trong lộ trình Transaction Microservice nâng cao.
> Mục tiêu: Hiểu sâu Outbox Pattern — pattern cốt lõi đảm bảo "ghi DB và publish event là atomic" trong môi trường không có distributed transaction. Đây là **chìa khóa kỹ thuật** giữ cho Saga chạy đúng trong thực tế.

---

## 0. Nhắc lại context

- **Bài 4:** Saga giải quyết transaction xuyên service bằng chuỗi local transaction + compensation.
- **Vấn đề còn lại:** Mỗi bước Saga cần làm 2 việc — `ghi DB local` + `publish event để bước sau chạy`. Nếu 2 việc này không atomic → Saga gãy.

→ Bài này giải vấn đề đó. Đây là pattern **bắt buộc phải có** trong mọi production-grade microservices system.

---

## 1. Dual Write Problem — đào sâu

### 1.1. Vấn đề cụ thể

Code "ngây thơ" mà ai cũng từng viết:

```java
@Service
public class OrderService {
    @Transactional
    public Order createOrder(OrderRequest req) {
        Order order = orderRepository.save(new Order(req));  // (1) Write DB
        kafkaTemplate.send("order-events", new OrderCreatedEvent(order));  // (2) Write Kafka
        return order;
    }
}
```

Câu hỏi: **Code trên có atomic không?**

Trả lời: **KHÔNG.** Lý do: `@Transactional` chỉ bao quanh DB. Kafka producer **không nằm trong** transaction đó.

### 1.2. 4 kịch bản có thể xảy ra

| Scenario | DB | Kafka | Hậu quả |
|---|---|---|---|
| Happy | OK | OK | Bình thường |
| **A** | OK | FAIL | DB có order, không ai biết → Saga ngừng tại đó, inventory không reserve, user thấy "order đang xử lý" mãi mãi |
| **B** | FAIL | OK | Event đã phát, các service xuống dòng xử lý "order ma" → inventory bị trừ, payment bị charge cho order không tồn tại |
| Both fail | FAIL | FAIL | Không có gì xảy ra (đỡ nhất) |

→ **Scenario A và B đều thảm họa.**

### 1.3. Tại sao không thể giải bằng cách "sửa thứ tự"?

**Thử 1: Send Kafka trước, save DB sau**
```java
kafkaTemplate.send(...);
orderRepository.save(order);
```
→ Vẫn fail: Kafka OK, DB FAIL → Scenario B.

**Thử 2: Send Kafka SAU khi commit**
```java
@Transactional
public Order createOrder(...) {
    Order order = orderRepository.save(...);
    return order;
}
// caller:
Order o = orderService.createOrder(req);  // commit xong
kafkaTemplate.send(o);  // send sau
```
→ Vẫn fail: DB OK, app crash trước khi send Kafka → Scenario A.

**Thử 3: Try-catch và rollback bằng tay**
```java
try {
    orderRepository.save(order);
    kafkaTemplate.send(...);
} catch (KafkaException e) {
    orderRepository.delete(order);  // "rollback"
}
```
→ Sai luôn:
- DELETE cũng có thể fail
- Network timeout: Kafka **đã** nhận message nhưng response không về → tưởng fail và rollback DB → Scenario B
- Đây gọi là **"2 Generals Problem"** — bài toán đã chứng minh không có lời giải nếu không có persistent state

**Thử 4: 2PC (XA) giữa DB và Kafka**
→ Kafka không support XA chuẩn. Hết đường.

### 1.4. Kết luận
**Không có cách nào giải Dual Write bằng cách "khéo léo sắp xếp code".** Phải có **kiến trúc** giải quyết. Đó là **Outbox Pattern**.

---

## 2. Outbox Pattern — Ý tưởng cốt lõi

### 2.1. Insight chính

> Thay vì ghi DB + send Kafka (2 hệ thống, không atomic được), hãy ghi DB + ghi "event vào DB" (cùng 1 hệ thống, ACID lo). Rồi 1 process khác đọc event từ DB và publish sang Kafka.

Tách biệt:
- **Business transaction**: ghi business data + ghi event vào **cùng 1 DB** → atomic (ACID lo)
- **Event publishing**: 1 process riêng đọc event từ DB → publish sang Kafka

### 2.2. Sơ đồ flow

```
                    ┌─────────────────────────────────────┐
                    │       OrderService DB               │
                    │  ┌──────────┐    ┌──────────────┐  │
   ┌──────────┐     │  │  orders  │    │ outbox_events│  │
   │  Client  │────►│  ├──────────┤    ├──────────────┤  │
   └──────────┘     │  │   ...    │    │     ...      │  │
        ▲           │  └──────────┘    └──────────────┘  │
        │           └──────────────────────────┬──────────┘
        │                                      │
        │                                      ▼  (poll / CDC)
        │                          ┌─────────────────────┐
        │                          │   Outbox Relay      │
        │                          │  (Debezium / Poller)│
        │                          └──────────┬──────────┘
        │                                     │
        │                                     ▼
        │                          ┌─────────────────────┐
        └──────────────────────────│       Kafka         │
                                   └─────────────────────┘
```

### 2.3. Bảng `outbox_events` (cấu trúc điển hình)

```sql
CREATE TABLE outbox_events (
    id              UUID PRIMARY KEY,
    aggregate_type  VARCHAR(100) NOT NULL,   -- "Order", "Payment"...
    aggregate_id    VARCHAR(100) NOT NULL,   -- ID của entity (order_id)
    event_type      VARCHAR(100) NOT NULL,   -- "OrderCreated", "OrderCancelled"
    payload         JSONB NOT NULL,          -- nội dung event
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    processed_at    TIMESTAMP NULL,          -- null = chưa publish; có giá trị = đã publish
    version         INT NOT NULL DEFAULT 1
);

CREATE INDEX idx_outbox_unprocessed 
    ON outbox_events(processed_at, created_at) 
    WHERE processed_at IS NULL;
```

### 2.4. Code chuẩn (Polling Publisher)

**Business transaction:**
```java
@Service
public class OrderService {
    @Transactional  // 1 transaction duy nhất, ACID đảm bảo
    public Order createOrder(OrderRequest req) {
        Order order = orderRepository.save(new Order(req));
        
        // Ghi event vào CÙNG DB, CÙNG transaction
        outboxRepository.save(OutboxEvent.builder()
            .aggregateType("Order")
            .aggregateId(order.getId().toString())
            .eventType("OrderCreated")
            .payload(toJson(order))
            .build());
            
        return order;
    }
}
```

**Outbox Relay (scheduled poller):**
```java
@Component
public class OutboxRelay {
    
    @Scheduled(fixedDelay = 500)  // 500ms
    public void publishOutboxEvents() {
        List<OutboxEvent> events = outboxRepository
            .findUnprocessed(PageRequest.of(0, 100));
        
        for (OutboxEvent event : events) {
            try {
                kafkaTemplate.send(
                    topicFor(event.getEventType()),
                    event.getAggregateId(),  // partition key
                    event.getPayload()
                ).get();  // wait for ack
                
                event.markProcessed();
                outboxRepository.save(event);
            } catch (Exception e) {
                log.error("Failed to publish event {}", event.getId(), e);
                // sẽ retry ở lần poll sau
                break;  // hoặc continue tùy chính sách
            }
        }
    }
}
```

→ **Đảm bảo at-least-once delivery**: event được publish ít nhất 1 lần. Có thể nhiều hơn (sẽ giải bằng Idempotency ở bài 6).

---

## 3. Hai cách implement Outbox Relay

### 3.1. Polling Publisher (đơn giản, kéo từ DB)

**Nguyên lý:** Cron/scheduled job query bảng `outbox_events WHERE processed_at IS NULL`, publish, mark processed.

**Ưu điểm:**
- Đơn giản, không cần infra phức tạp
- Hoàn toàn nằm trong app, dễ debug
- Phù hợp scale nhỏ-vừa

**Nhược điểm:**
- **Latency**: phụ thuộc poll interval (500ms-1s là tối thiểu thực tế)
- **DB load**: polling liên tục → query liên tục
- **Race condition** với multiple instance: 2 pod cùng poll → publish trùng → cần lock
- **Throughput hạn chế**: với 10k events/s thì polling không kịp

**Concurrency control khi multi-instance:**

Cách A — Pessimistic lock (PostgreSQL):
```sql
SELECT * FROM outbox_events 
WHERE processed_at IS NULL 
ORDER BY created_at 
LIMIT 100
FOR UPDATE SKIP LOCKED;  -- key: SKIP LOCKED
```
Pod khác sẽ skip những row đang bị lock, không bị block.

Cách B — Leader election: chỉ 1 instance làm relay tại 1 thời điểm (dùng ShedLock, Zookeeper, Redis).

Cách C — Phân shard theo `aggregate_id`: pod 1 xử lý hash%N==0, pod 2 xử lý hash%N==1, v.v.

### 3.2. Transaction Log Tailing / CDC (Debezium)

**Nguyên lý:** Đọc trực tiếp **WAL (Write-Ahead Log)** của database, capture mọi INSERT vào bảng `outbox_events`, stream sang Kafka.

```
PostgreSQL WAL ──► Debezium Connector ──► Kafka
   (logical 
    replication)
```

**Cấu hình mẫu Debezium (PostgreSQL):**
```json
{
  "name": "outbox-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.dbname": "orderdb",
    "table.include.list": "public.outbox_events",
    "plugin.name": "pgoutput",
    "transforms": "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.table.field.event.key": "aggregate_id",
    "transforms.outbox.route.by.field": "aggregate_type"
  }
}
```

**Ưu điểm:**
- **Latency cực thấp** (ms): đọc WAL real-time
- **Không load DB**: WAL replication được DB optimize
- **Không lo race condition**: Debezium đảm bảo ordered, exactly-once delivery sang Kafka
- **Scale mạnh**: handle hàng trăm nghìn events/s
- **Production-grade**: Netflix, LinkedIn, Uber dùng pattern này

**Nhược điểm:**
- **Infra phức tạp hơn**: cần Kafka Connect cluster, Debezium connector
- **Phụ thuộc DB**: cần DB hỗ trợ logical replication (PostgreSQL từ v9.4, MySQL với binlog, MongoDB change streams)
- **Cần ops experience**: monitor offsets, handle connector failure
- **Schema evolution**: khi thay đổi schema event phải cẩn thận

### 3.3. So sánh chọn cái nào

| | Polling | CDC (Debezium) |
|---|---|---|
| Setup | Đơn giản | Phức tạp |
| Latency | 500ms - 1s | < 100ms |
| Throughput | Hàng nghìn/s | Hàng trăm nghìn/s |
| DB load | Có | Không đáng kể |
| Infra | Chỉ app + DB | + Kafka Connect cluster |
| Phù hợp | Startup, MVP, scale nhỏ | Enterprise, scale lớn |
| Maintenance | Code app | Ops Kafka Connect |

→ **Lộ trình thực tế:**
1. Bắt đầu với **Polling** để hiểu pattern và launch nhanh
2. Khi scale tăng, throughput cao → **migrate sang Debezium**
3. Cấu trúc bảng `outbox_events` giống nhau → migrate dễ

---

## 4. Cleanup Outbox — Vấn đề bị bỏ quên

Sau khi event đã publish, bảng `outbox_events` chỉ phình to mãi → ảnh hưởng performance.

### 4.1. Các chiến lược cleanup

**Chiến lược 1: Delete ngay sau publish**
```java
event.markProcessed();
outboxRepository.delete(event);
```
- **Ưu**: bảng luôn nhỏ
- **Nhược**: mất audit trail; nếu cần replay event (recovery, debug) thì bí

**Chiến lược 2: Soft delete + cron cleanup**
```java
@Scheduled(cron = "0 0 3 * * *")  // 3 AM mỗi ngày
public void cleanupProcessed() {
    outboxRepository.deleteOlderThan(LocalDateTime.now().minusDays(7));
}
```
- **Ưu**: giữ event 1 tuần để debug/replay
- **Nhược**: bảng vẫn lớn, cần index tốt

**Chiến lược 3: Move sang archive table**
```sql
INSERT INTO outbox_events_archive SELECT * FROM outbox_events 
  WHERE processed_at < now() - interval '7 days';
DELETE FROM outbox_events WHERE processed_at < now() - interval '7 days';
```
- **Ưu**: archive table có thể partition theo tháng
- **Nhược**: phức tạp hơn, cần 2 bảng

**Chiến lược 4 (Debezium): không lưu lâu trong DB**
Vì Kafka đã là source of truth cho event log → có thể delete row ngay sau khi Debezium đọc xong (Debezium có config cho việc này).

### 4.2. Khuyến nghị

- **Polling**: dùng chiến lược 2 (soft delete + cron)
- **Debezium**: dùng chiến lược 1 hoặc 4 (Kafka đã giữ lịch sử)

---

## 5. Inbox Pattern — Cặp đôi của Outbox

### 5.1. Vấn đề ngược lại

Phía **consumer** cũng có vấn đề:
```java
@KafkaListener(topics = "order-events")
public void handle(OrderCreatedEvent e) {
    inventoryService.reserve(e);  // (1) business logic
    kafkaTemplate.send("inventory-events", new StockReservedEvent(...));  // (2) publish next event
}
```

Tương tự dual write: (1) OK + (2) FAIL → bí.

### 5.2. Inbox Pattern

> Khi consume message, ghi vào bảng `inbox_events` trước (như acknowledgement), rồi xử lý business + outbox như bình thường.

```sql
CREATE TABLE inbox_events (
    message_id  VARCHAR(100) PRIMARY KEY,  -- unique từ producer (Kafka offset hoặc UUID)
    received_at TIMESTAMP NOT NULL,
    processed   BOOLEAN NOT NULL DEFAULT false
);
```

**Code mẫu:**
```java
@KafkaListener(topics = "order-events")
@Transactional
public void handle(OrderCreatedEvent e, @Header("message-id") String msgId) {
    // 1. Idempotency check (sẽ học chi tiết bài 6)
    if (inboxRepository.existsByMessageId(msgId)) {
        log.info("Already processed {}", msgId);
        return;  // ack nhưng không làm gì
    }
    
    // 2. Ghi inbox
    inboxRepository.save(new InboxEvent(msgId));
    
    // 3. Business logic
    inventoryService.reserve(e);
    
    // 4. Outbox cho event tiếp theo
    outboxRepository.save(new OutboxEvent("StockReserved", ...));
    
    // Tất cả trong 1 transaction → atomic
}
```

→ **Outbox + Inbox** = bộ đôi đảm bảo **exactly-once processing** từ đầu đến cuối chuỗi Saga.

---

## 6. Outbox và Saga — Bức tranh tổng thể

Áp dụng vào flow `Order → Inventory → Payment → Shipping`:

```
┌─────────────────────────────────────────────────────────────────┐
│ OrderService                                                    │
│   @Transactional {                                              │
│     INSERT INTO orders ...                                      │
│     INSERT INTO outbox_events ('OrderCreated', ...)             │
│   }                                                             │
└─────────────────────────────────────────────────────────────────┘
                              │
                  Outbox Relay (Polling/Debezium)
                              │
                              ▼
                   ┌─────────────────┐
                   │ Kafka topic     │
                   │ "order-events"  │
                   └────────┬────────┘
                            │
┌───────────────────────────┴──────────────────────────────────────┐
│ InventoryService                                                  │
│   @KafkaListener                                                  │
│   @Transactional {                                                │
│     INSERT INTO inbox_events (msg_id) -- idempotency check        │
│     UPDATE inventory SET reserved += qty                          │
│     INSERT INTO outbox_events ('StockReserved', ...)              │
│   }                                                               │
└───────────────────────────────────────────────────────────────────┘
                              │
                  Outbox Relay (Polling/Debezium)
                              │
                              ▼
                   ┌─────────────────┐
                   │ Kafka topic     │
                   │"inventory-events"
                   └────────┬────────┘
                            │
                          ... (Payment, Shipping tương tự)
```

→ Mỗi service: **3 thành phần local**: business table + inbox + outbox. Mọi event flow: **outbox → Kafka → inbox → business → outbox tiếp theo**.

→ Đây là **kiến trúc chuẩn** cho microservices event-driven hiện đại.

---

## 7. Listen-to-Yourself Pattern — Biến thể

Một biến thể đẹp khi service vừa producer vừa consumer:

```java
// OrderService publish "OrderCreated"
// OrderService cũng listen "OrderCreated" để update status
```

→ Khi event được publish thành công sang Kafka, service consume lại event của chính mình → update local state (vd: `order.status = SAGA_STARTED`).

→ Lợi ích: tất cả state transition đều **đi qua event log** → có audit, có replay capability.

→ Hơi over-engineering cho hệ thống nhỏ, nhưng rất mạnh cho Event Sourcing (sẽ học sau lộ trình này).

---

## 8. So sánh với các giải pháp khác

| Giải pháp | Atomic DB+Event? | Latency | Complexity | Sử dụng |
|---|---|---|---|---|
| Naive (save + send) | KHÔNG | Thấp | Thấp | Sai, đừng dùng |
| 2PC/XA | Có (về lý thuyết) | Cao | Cao | Kafka không hỗ trợ |
| **Outbox + Polling** | **Có (eventual)** | **TB** | **TB** | **MVP, scale nhỏ-vừa** |
| **Outbox + CDC (Debezium)** | **Có (eventual)** | **Thấp** | **Cao** | **Production scale** |
| Event Sourcing | Có | TB | Rất cao | Hệ thống cần audit mạnh |
| Transactional Outbox (Kafka 0.11+ exactly-once) | Một phần | Thấp | TB | Khi cả DB+Kafka đều "Kafka-native" |

---

## 9. Bẫy phổ biến

### Bẫy 1: Quên @Transactional bao quanh save business + save outbox
Bỏ `@Transactional` → 2 INSERT chạy 2 connection khác nhau → fail 1 cái không rollback cái kia → **mất ý nghĩa của Outbox**.

### Bẫy 2: Outbox Relay không retry khi Kafka down
Kafka tạm thời down → relay throw exception → các event sau cũng không publish được. Cần:
- Retry với exponential backoff
- Đánh dấu `retry_count` trên event
- Sau N lần fail → đưa vào DLQ hoặc cảnh báo manual

### Bẫy 3: Không có ordering guarantee
Nếu publish theo `created_at ASC` nhưng không lock → 2 instance publish out-of-order. Giải pháp: 
- `SKIP LOCKED` + lock theo `aggregate_id`
- Hoặc dùng Debezium (đảm bảo order theo WAL)
- Partition Kafka theo `aggregate_id` → ordering trong cùng aggregate được đảm bảo

### Bẫy 4: Outbox table không index → query polling chậm dần
Phải có **partial index** trên `WHERE processed_at IS NULL`. Nếu không, scan full table mỗi lần poll → chậm theo cấp số.

### Bẫy 5: Payload quá lớn trong outbox
Outbox table nằm trong DB chính → ảnh hưởng IO. Đừng nhét full PDF, image vào payload. Nếu cần, lưu URL/reference.

### Bẫy 6: Schema event không versioned
Sửa event schema phá vỡ consumer cũ → catastrophic. Outbox event phải có `schema_version` từ đầu, design backward-compatible.

### Bẫy 7: Quên đảm bảo Idempotency phía consumer
Outbox đảm bảo at-least-once → consumer **chắc chắn sẽ nhận trùng**. Không có idempotency phía consumer → trừ kho 2 lần, charge 2 lần. → Inbox + Idempotency là **mandatory**, không phải "nice to have".

### Bẫy 8: Dùng `@TransactionalEventListener` nghĩ là đủ
Spring có `@TransactionalEventListener(phase = AFTER_COMMIT)` — listener chạy sau commit. Mới nhìn tưởng giải được Dual Write.
→ **Sai.** Nếu app crash giữa commit và listener chạy → event bị mất. Đây không phải Outbox. Outbox phải **persisted**, không phải in-memory event.

---

## 10. Cầu nối tới bài tiếp theo

Bạn đã hiểu cách đảm bảo **producer-side**: event chắc chắn được publish (at-least-once).

Nhưng đã thấy: **at-least-once** = event có thể nhận trùng. Nếu consumer không handle trùng → trừ kho 2 lần.

→ Bài tiếp theo: **Idempotency + Exactly-Once Semantics + Inbox Pattern chi tiết** — để Saga đảm bảo "đúng 1 lần effective" dù message đến nhiều lần.

---

## 11. Gợi ý thực hành

1. Tự code Outbox với Polling: 1 PostgreSQL, 1 Spring Boot service, scheduled poller. Kill app giữa transaction để confirm atomicity.
2. Thử với multi-instance: 2 pod cùng poll, dùng `FOR UPDATE SKIP LOCKED`, confirm không duplicate.
3. Cài Debezium + Kafka Connect, config CDC cho outbox table, đo latency so với polling.
4. Đọc article gốc của Chris Richardson: "Pattern: Transactional Outbox" trên microservices.io
5. Đọc Debezium tutorial về Outbox Event Router transformation.
6. Đọc bài "Reliable Microservices Data Exchange With the Outbox Pattern" trên Red Hat Developer blog.
7. Xem demo Debezium từ team Confluent trên YouTube — rất chi tiết.

---

## Checklist tự đánh giá

- [ ] Giải thích Dual Write Problem với 4 scenario cụ thể
- [ ] Vì sao "khéo léo sắp xếp code" không giải được Dual Write
- [ ] Vẽ kiến trúc Outbox Pattern (DB table + Relay + Kafka)
- [ ] Viết được schema bảng outbox_events đầy đủ index
- [ ] Phân biệt Polling Publisher và CDC (Debezium), biết khi nào dùng cái nào
- [ ] Giải quyết race condition khi multi-instance polling (SKIP LOCKED, leader election)
- [ ] Hiểu Inbox Pattern và lý do tồn tại
- [ ] Vẽ flow Outbox+Inbox áp dụng vào toàn Saga (Order → Inventory → Payment → Shipping)
- [ ] Biết cleanup strategy phù hợp với từng implementation
- [ ] Liệt kê 8 bẫy phổ biến với Outbox
- [ ] Giải thích vì sao `@TransactionalEventListener` KHÔNG phải Outbox

---

**Bài tiếp theo:** `06 - Idempotency, Exactly-Once Semantics và Inbox Pattern chi tiết`
