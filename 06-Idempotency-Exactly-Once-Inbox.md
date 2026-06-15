# Idempotency, Exactly-Once Semantics và Inbox Pattern chi tiết

> Bài 6 trong lộ trình Transaction Microservice nâng cao.
> Mục tiêu: Hiểu sâu Idempotency — tính chất bắt buộc của mọi consumer trong event-driven system. Phân biệt 3 mức delivery semantics (at-most-once, at-least-once, exactly-once). Biết cách thiết kế consumer chịu được duplicate, out-of-order, và replay.

---

## 0. Nhắc lại context

- **Bài 5:** Outbox đảm bảo event **at-least-once** từ producer sang Kafka.
- **Hệ quả:** Consumer **chắc chắn sẽ nhận duplicate** — không phải nếu mà là khi nào.
- **Bài 6 (bài này):** Làm sao xử lý duplicate đúng cách, để hành động cuối cùng giống như chỉ xảy ra 1 lần.

→ Đây là pattern **bắt buộc** cho mọi consumer trong Saga. Không có idempotency = Saga sai.

---

## 1. Delivery Semantics — 3 mức giao hàng

Trong message-driven system có 3 mức "đảm bảo giao hàng":

### 1.1. At-most-once (≤ 1 lần)

> Message có thể bị **mất**, nhưng **không bao giờ duplicate**.

- Producer gửi xong → quên, không retry
- Consumer nhận → ack ngay khi nhận → xử lý sau
- Nếu consumer crash sau khi ack mà chưa xử lý → message mất

**Khi nào dùng:**
- Metrics, logs (mất vài cái không sao)
- Real-time gaming state (state mới sẽ override state cũ)
- Sensor data với rate cao

**Trade-off:** Đơn giản nhất, nhanh nhất, nhưng **không reliable**.

### 1.2. At-least-once (≥ 1 lần)

> Message **chắc chắn được giao**, nhưng có thể **duplicate**.

- Producer retry cho đến khi có ack
- Consumer ack **sau khi** xử lý xong
- Nếu consumer crash giữa xử lý và ack → broker redeliver

**Khi nào dùng:**
- **Mặc định của hầu hết hệ thống** (Kafka, RabbitMQ default)
- E-commerce, banking, hầu hết business logic
- **Bắt buộc**: consumer phải idempotent

**Trade-off:** Reliable, nhưng đẩy gánh nặng "không xử lý trùng" sang consumer.

### 1.3. Exactly-once (= 1 lần)

> Message được giao **đúng 1 lần**, không mất, không trùng.

- **Lý thuyết:** Không tồn tại trong distributed system (CAP, Two Generals)
- **Thực tế:** Đạt được bằng cách kết hợp at-least-once delivery + idempotent processing
- Còn gọi là **"effectively exactly-once"** hoặc **"exactly-once semantics" (EOS)**

→ Đây là **mục tiêu** của mọi production system. Đạt được như sau:

```
Exactly-once = At-least-once delivery + Idempotent processing
```

**Kafka exactly-once support (từ 0.11+):**
- Idempotent producer (transactional.id)
- Read-process-write trong Kafka transaction
- → Chỉ exactly-once **trong phạm vi Kafka**, không xuyên sang DB

→ Vẫn cần idempotency ở consumer khi consumer ghi xuống DB.

---

## 2. Idempotency là gì

### 2.1. Định nghĩa toán học

> Một thao tác `f` là **idempotent** nếu `f(f(x)) = f(x)`. Tức là chạy nhiều lần kết quả giống chạy 1 lần.

### 2.2. Trong context API/microservice

> Một request/message là **idempotent** nếu xử lý nó nhiều lần cho cùng kết quả như xử lý 1 lần.

### 2.3. Ví dụ

**Idempotent (✓):**
```sql
UPDATE users SET status = 'ACTIVE' WHERE id = 5;
```
Chạy 1 lần hay 100 lần → kết quả giống nhau.

```sql
DELETE FROM orders WHERE id = 10;
```
Chạy lần 2 không xoá thêm gì → vẫn idempotent.

**Không idempotent (✗):**
```sql
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
```
Chạy 2 lần → trừ 200. Đây là **vấn đề chí mạng** trong banking.

```sql
INSERT INTO payments (order_id, amount) VALUES (1, 100);
```
Chạy 2 lần → 2 payment record cho cùng order.

### 2.4. Idempotency ở các tầng

| Tầng | Hành động | Idempotent? | Cách làm idempotent |
|---|---|---|---|
| HTTP | GET, PUT, DELETE | Có | Bản thân spec |
| HTTP | POST | Không | Idempotency-Key header |
| SQL | UPDATE absolute (SET=X) | Có | - |
| SQL | UPDATE relative (SET=X+1) | Không | Version check / state check |
| SQL | INSERT | Không | UNIQUE constraint / UPSERT |
| Kafka | Consume | Không | Inbox / dedup |

---

## 3. Vì sao Idempotency là bắt buộc trong Saga

Quay lại flow Saga của bạn:

```
Outbox (at-least-once) → Kafka → Consumer
```

Kịch bản duplicate có thể xảy ra:

### Scenario 1: Outbox relay publish 2 lần
- Relay publish sang Kafka thành công nhưng network timeout khi nhận ack
- Relay nghĩ fail → publish lại
- → Kafka nhận 2 lần

### Scenario 2: Consumer crash sau xử lý, trước commit offset
- Consumer xử lý message X (trừ kho, ghi DB)
- Chưa commit offset thì crash
- Restart → nhận lại message X → xử lý lại
- → Trừ kho 2 lần

### Scenario 3: Rebalance trong Kafka
- Consumer group có 3 pod. 1 pod crash → rebalance
- Một số partition được reassign → consumer mới có thể nhận lại message chưa commit
- → Duplicate

### Scenario 4: Producer retry
- Producer gửi message, broker nhận nhưng ack bị mất
- Producer retry → message đến broker 2 lần
- (Kafka idempotent producer giảm được, nhưng không 0)

→ **Không phải vấn đề nếu, mà vấn đề khi nào.** Mọi consumer phải sẵn sàng cho duplicate.

---

## 4. Các kỹ thuật làm Consumer Idempotent

### 4.1. Kỹ thuật 1: Inbox Pattern (Message Deduplication Table)

Đã giới thiệu ở bài 5, đây là cách **phổ biến nhất**.

```sql
CREATE TABLE inbox_events (
    message_id      VARCHAR(100) PRIMARY KEY,  -- ID unique của message
    consumer_group  VARCHAR(100) NOT NULL,     -- tên consumer (cùng message, nhiều consumer)
    received_at     TIMESTAMP NOT NULL DEFAULT now(),
    processed_at    TIMESTAMP NULL,
    payload_hash    VARCHAR(64) NULL           -- optional, để verify
);
```

**Code chuẩn:**
```java
@KafkaListener(topics = "order-events", groupId = "inventory-service")
@Transactional
public void handle(OrderCreatedEvent event, @Header("message-id") String msgId) {
    // 1. Idempotency check
    if (inboxRepository.existsByMessageIdAndConsumerGroup(msgId, "inventory-service")) {
        log.info("Message {} already processed, skipping", msgId);
        return;  // ack Kafka, nhưng không làm gì
    }
    
    // 2. Ghi inbox TRƯỚC
    inboxRepository.save(InboxEvent.builder()
        .messageId(msgId)
        .consumerGroup("inventory-service")
        .build());
    
    // 3. Business logic
    inventoryService.reserve(event);
    
    // 4. Outbox cho event tiếp theo
    outboxRepository.save(new OutboxEvent("StockReserved", ...));
    
    // 5. Commit transaction → inbox + business + outbox cùng atomic
    // 6. Sau commit, Spring Kafka tự ack
}
```

**Tại sao cần `@Transactional` ôm cả 3 bước:**
- Nếu ghi inbox xong mà business fail → inbox rollback → lần sau retry vẫn xử lý lại được
- Nếu business OK mà outbox fail → tất cả rollback → giữ atomicity

**Message ID lấy từ đâu:**
- Producer tự sinh UUID khi tạo outbox event, gửi qua header `message-id`
- Hoặc dùng `topic-partition-offset` (Kafka)
- Hoặc dùng `aggregate_id + event_type + version` (event sourcing style)

**Cleanup inbox:**
- Tương tự outbox: cron job xóa record cũ hơn 7-30 ngày
- Không xóa hết — vì duplicate có thể đến **trễ** (rebalance, replay)

### 4.2. Kỹ thuật 2: Idempotency-Key (HTTP/API style)

Khi client gọi API, gửi `Idempotency-Key` header:

```http
POST /api/orders
Idempotency-Key: 7b3e1a8d-4f9c-...
Content-Type: application/json

{ "items": [...], "total": 100 }
```

Server:
```java
@PostMapping("/orders")
public ResponseEntity<Order> create(
    @RequestHeader("Idempotency-Key") String key,
    @RequestBody OrderRequest req) {
    
    // 1. Check cache (Redis) hoặc DB
    Optional<Order> existing = idempotencyRepo.findByKey(key);
    if (existing.isPresent()) {
        return ResponseEntity.ok(existing.get());  // return cached response
    }
    
    // 2. Tạo order mới
    Order order = orderService.create(req);
    idempotencyRepo.save(new IdempotencyRecord(key, order));
    
    return ResponseEntity.ok(order);
}
```

**Trường hợp dùng:**
- Payment API (Stripe, Adyen dùng pattern này)
- Mobile app retry khi network không ổn
- Frontend double-click submit

**TTL:** thường 24h - 7 ngày, sau đó key cũ bị xóa.

### 4.3. Kỹ thuật 3: Natural Idempotency (qua DB constraint)

Dùng UNIQUE constraint để DB tự chặn duplicate:

```sql
CREATE TABLE payments (
    id           UUID PRIMARY KEY,
    order_id     UUID NOT NULL,
    amount       DECIMAL NOT NULL,
    UNIQUE (order_id)  -- 1 order chỉ có 1 payment
);
```

Code:
```java
try {
    paymentRepository.save(payment);
} catch (DataIntegrityViolationException e) {
    // Đã tồn tại → đây là duplicate → bỏ qua
    log.info("Payment for order {} already exists", orderId);
    return paymentRepository.findByOrderId(orderId);
}
```

**Khi nào dùng:**
- Có natural key (order_id, transaction_id từ payment gateway)
- Đơn giản, không cần thêm bảng

**Hạn chế:**
- Phải catch exception (kém clean)
- Một số case business cho phép nhiều record (vd: nhiều payment attempt cho 1 order)

### 4.4. Kỹ thuật 4: UPSERT / Conditional Update

PostgreSQL:
```sql
INSERT INTO inventory (product_id, available)
VALUES (:product_id, :qty)
ON CONFLICT (product_id) 
DO UPDATE SET available = inventory.available - :qty
WHERE inventory.available >= :qty;
```

Hoặc với event versioning:
```sql
UPDATE orders 
SET status = 'CONFIRMED', version = version + 1
WHERE id = :order_id 
  AND version = :expected_version;  -- chỉ update nếu version đúng
```

Nếu affected rows = 0 → đã có ai update trước → đây là duplicate hoặc out-of-order.

### 4.5. Kỹ thuật 5: State-based Idempotency

Kiểm tra **state hiện tại** trước khi action:

```java
public void confirmOrder(UUID orderId) {
    Order order = orderRepository.findById(orderId).orElseThrow();
    
    if (order.getStatus() == OrderStatus.CONFIRMED) {
        return;  // đã confirm rồi → idempotent
    }
    
    if (order.getStatus() != OrderStatus.PENDING) {
        throw new IllegalStateException("Cannot confirm order in status " + order.getStatus());
    }
    
    order.confirm();
    orderRepository.save(order);
}
```

→ Đặc biệt hữu ích cho state machine: chỉ chuyển trạng thái nếu đang ở state hợp lệ.

### 4.6. So sánh các kỹ thuật

| Kỹ thuật | Khi nào dùng | Ưu | Nhược |
|---|---|---|---|
| **Inbox** | Mọi Kafka consumer | Universal, atomic | Cần thêm bảng |
| **Idempotency-Key** | Public HTTP API | Client-controlled, RESTful | Client phải implement |
| **Natural Idempotency** | Có natural key | Đơn giản | Phụ thuộc DB schema |
| **UPSERT** | Bulk update | Performant | Phức tạp SQL |
| **State-based** | Workflow/state machine | Business-meaningful | Cần state field |

→ **Trong Saga thường kết hợp Inbox + State-based:**
- Inbox: chống duplicate message
- State-based: chống out-of-order, chống late event

---

## 5. Idempotency cho Compensating Transaction

**Đây là điểm Senior hay quên.** Đã nói ở bài 4 nhưng nhắc lại vì cực quan trọng.

Compensation cũng được gửi qua Kafka → cũng có thể duplicate. Mà nếu compensation không idempotent:

```java
// SAI:
public void releaseStock(Long productId, int qty) {
    inventory.setReserved(inventory.getReserved() - qty);
}
```

Duplicate 2 lần → release 2 lần → reserved âm → khi user khác mua sẽ thấy còn hàng nhưng thực ra hết.

```java
// ĐÚNG (state-based):
@Transactional
public void releaseStock(String reservationId) {
    Reservation res = reservationRepo.findById(reservationId).orElseThrow();
    
    if (res.getStatus() == ReservationStatus.RELEASED) {
        return;  // idempotent
    }
    
    inventory.setReserved(inventory.getReserved() - res.getQty());
    res.setStatus(ReservationStatus.RELEASED);
}
```

→ Mỗi reservation có ID riêng, release dựa trên ID, đã release rồi thì skip.

---

## 6. Out-of-Order Events — Vấn đề liên quan

### 6.1. Vấn đề

Kafka đảm bảo order **trong cùng partition**, không xuyên partition. Nếu events đi qua nhiều partition → có thể out-of-order.

Ví dụ:
- T0: PaymentCompleted (partition 1)
- T1: PaymentRefunded (partition 2)
- Consumer nhận: PaymentRefunded **trước**, PaymentCompleted sau
- → Logic sai: refund cho payment chưa tồn tại

### 6.2. Giải pháp

**Giải pháp 1: Partition key đúng**
```java
kafkaTemplate.send("payment-events", 
    paymentId.toString(),  // partition key
    event);
```
→ Cùng paymentId → cùng partition → order đảm bảo.

**Giải pháp 2: Version / Sequence trong event**

Mỗi event có `version` hoặc `sequence_no` tăng dần:
```json
{
  "eventType": "PaymentCompleted",
  "paymentId": "abc",
  "version": 5,
  "payload": {...}
}
```

Consumer check:
```java
if (event.getVersion() <= lastProcessedVersion(event.getAggregateId())) {
    return;  // out-of-order hoặc duplicate, skip
}
processEvent(event);
updateLastProcessedVersion(event.getAggregateId(), event.getVersion());
```

**Giải pháp 3: Buffer + Reorder**
Nếu thực sự cần process theo thứ tự nhưng event đến không thứ tự → buffer trong consumer, sort theo version, process khi đủ.
→ Phức tạp, hiếm dùng.

**Giải pháp 4: Design event để không quan trọng order**
- Dùng absolute state thay vì delta
- Compensation idempotent
- Late event coming → reconciliation job

---

## 7. Replay — Event Sourcing taste

Đôi khi cần replay events: 
- Sửa bug rồi muốn re-process toàn bộ event
- Khôi phục state từ event log
- Build read model mới

→ Nếu consumer **đã idempotent**, replay là **an toàn** — chạy lại event cũ không phá data.

→ Đây là siêu năng lực: idempotency biến event log thành **source of truth** có thể replay.

Cách trigger replay:
- Kafka: reset offset về đầu (hoặc về thời điểm cụ thể)
- Consumer xử lý lại tất cả → vì idempotent nên OK

→ Nếu chưa idempotent → tuyệt đối **không** replay → sẽ catastrophic.

---

## 8. Kafka Exactly-Once Semantics (EOS) — Có nên tin?

Kafka từ 0.11+ có "exactly-once" với:
```properties
enable.idempotence=true
transactional.id=my-tx-id
isolation.level=read_committed
```

Hỗ trợ:
- Producer gửi không duplicate trong cùng session
- Read-process-write atomic **trong phạm vi Kafka**

→ **Nhưng cẩn thận:** Kafka EOS chỉ exactly-once **giữa các topic Kafka**. Khi consumer ghi xuống **DB ngoài Kafka** (PostgreSQL, MongoDB...) → Kafka EOS **không bao trùm**.

→ Vẫn cần Inbox pattern khi DB là sink.

→ Kafka EOS hữu ích chủ yếu cho Kafka Streams (stream processing trong cùng Kafka).

---

## 9. Áp dụng vào flow của bạn

Mỗi service trong flow `Order → Inventory → Payment → Shipping` phải có:

### Bảng chuẩn của mỗi service:
```sql
-- Business tables
CREATE TABLE orders/inventory/payments/shipments (...);

-- Outbox (producer side)
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    aggregate_type VARCHAR(100),
    aggregate_id VARCHAR(100),
    event_type VARCHAR(100),
    payload JSONB,
    created_at TIMESTAMP,
    processed_at TIMESTAMP NULL
);

-- Inbox (consumer side)
CREATE TABLE inbox_events (
    message_id VARCHAR(100) PRIMARY KEY,
    consumer_group VARCHAR(100),
    received_at TIMESTAMP,
    processed_at TIMESTAMP NULL
);
```

### Skeleton chuẩn của mỗi consumer:
```java
@KafkaListener(topics = "...", groupId = "...")
@Transactional
public void onEvent(MyEvent event, @Header("message-id") String msgId) {
    // 1. Idempotency check
    if (inboxRepo.exists(msgId, "my-group")) return;
    
    // 2. State-based check (nếu áp dụng)
    Entity e = repo.findById(event.getAggregateId()).orElseThrow();
    if (!e.canTransitionTo(targetState)) return;
    
    // 3. Save to inbox
    inboxRepo.save(new InboxEvent(msgId, "my-group"));
    
    // 4. Business logic
    businessService.process(event);
    
    // 5. Outbox for next event
    outboxRepo.save(new OutboxEvent(...));
    
    // 6. @Transactional commit → all atomic
}
```

→ **Đây là template chuẩn** cho mọi consumer.

---

## 10. Bẫy phổ biến

### Bẫy 1: Idempotency check OUTSIDE transaction
```java
// SAI:
if (inboxRepo.exists(msgId)) return;  // check ngoài transaction

@Transactional
public void process(...) {
    inboxRepo.save(...);
    // ...
}
```
Race condition: 2 instance cùng check → cùng false → cùng process. Phải **trong cùng transaction** với INSERT inbox và dùng UNIQUE constraint trên `message_id` để bắt duplicate.

### Bẫy 2: Quên consumer_group trong inbox key
Nếu 1 topic có nhiều consumer group (vd: InventoryService và NotificationService cùng consume `order-events`), inbox primary key chỉ là `message_id` → một group đã process, group kia bị skip.
→ Primary key phải là `(message_id, consumer_group)`.

### Bẫy 3: Idempotency check chỉ ở app, không có DB constraint
App check OK rồi mới INSERT → race condition → 2 INSERT cùng lúc. **Phải có UNIQUE constraint** ở DB, app check chỉ là fast path.

### Bẫy 4: Cache idempotency-key trong Redis mà không backup DB
Redis evict / restart → key mất → duplicate qua lọt. Idempotency-key phải lưu **DB persistent**, Redis chỉ là cache.

### Bẫy 5: Compensation không idempotent
Lặp lại điểm này lần nữa vì cực quan trọng. Compensation cũng cần đầy đủ kỹ thuật idempotency.

### Bẫy 6: TTL idempotency key quá ngắn
Đặt TTL 1 giờ. Sau 2 giờ, retry đến → key đã expire → process lại → duplicate.
→ TTL phải dài hơn **time window tối đa** của duplicate (thường 24h-7 ngày tùy hệ thống).

### Bẫy 7: Tin Kafka EOS che hết
Như đã nói, Kafka EOS không bao trùm DB write. Đừng nghĩ "tôi đã bật EOS = không cần inbox".

### Bẫy 8: Idempotency check trên payload thay vì messageId
```java
// SAI:
if (orderRepo.existsByOrderId(event.getOrderId())) return;
```
Trông giống idempotent nhưng có vấn đề:
- Cùng orderId có thể có nhiều event hợp lệ (OrderCreated, OrderUpdated, OrderCancelled)
- Check theo orderId → skip cả event hợp lệ
→ **Idempotency phải theo message identity, không phải business identity.**

### Bẫy 9: Không monitor duplicate rate
Production cần monitor:
- Số lượng "skipped due to duplicate" per minute
- Tỉ lệ duplicate / total messages
- Nếu duplicate rate đột nhiên tăng → có bug producer / consumer rebalance bất thường

### Bẫy 10: Quên xử lý "poison message"
Message corrupt / bug → consumer luôn fail → retry vô tận → block toàn partition. Cần:
- Max retry count
- Dead Letter Queue (DLQ)
- Alert khi message vào DLQ

---

## 11. Cầu nối tới phần implementation

Bạn đã có **đầy đủ lý thuyết và pattern**:
1. ✓ ACID + Isolation (bài 1)
2. ✓ CAP + Eventual Consistency (bài 2)
3. ✓ 2PC/3PC và lý do bỏ (bài 3)
4. ✓ Saga: Orchestration vs Choreography (bài 4)
5. ✓ Outbox Pattern (bài 5)
6. ✓ Idempotency + Inbox + EOS (bài 6)

Đây là **toàn bộ "framework tư duy"** của một Senior Backend trong domain distributed transaction. Mọi system you build will use các pattern này.

**Tiếp theo (bài 7+)** sẽ là phần **implementation thực sự**:
- Setup project Spring Boot multi-module
- Setup Kafka + PostgreSQL với Docker Compose
- Implement Order Service đầy đủ Outbox + Inbox
- Implement Inventory, Payment, Shipping
- Implement Orchestrator với State Machine
- Failure injection: kill Kafka, kill DB, kill service giữa flow
- Monitoring: tracing, metrics, alerts
- Đóng gói thành demo có thể show cho phỏng vấn

---

## 12. Gợi ý thực hành

1. Tự viết 1 consumer Kafka không có idempotency. Gửi cùng message 5 lần, quan sát side effect (tạo 5 record, charge 5 lần).
2. Thêm Inbox pattern, gửi lại 5 lần, confirm chỉ xử lý 1 lần.
3. Code 1 endpoint `POST /payment` với `Idempotency-Key` header, test với cùng key 2 lần.
4. Cố tình tạo race condition: 2 thread cùng insert inbox với cùng message_id → confirm DB UNIQUE constraint bắt được.
5. Thử Kafka EOS: enable `transactional.id`, producer-consumer trong cùng Kafka transaction.
6. Thực hành replay: dump 1000 events vào Kafka, reset offset, consumer chạy lại, confirm không duplicate side effect nhờ idempotency.
7. Đọc article của Stripe: "Designing robust and predictable APIs with idempotency"
8. Đọc Confluent blog: "Exactly-Once Semantics Are Possible: Here's How Kafka Does It"
9. Đọc chương 11 "Stream Processing" trong **Designing Data-Intensive Applications**, phần Fault Tolerance.

---

## Checklist tự đánh giá

- [ ] Phân biệt 3 delivery semantics (at-most, at-least, exactly-once)
- [ ] Giải thích "Exactly-once = At-least-once + Idempotency"
- [ ] Định nghĩa Idempotency toán học và trong context API
- [ ] Liệt kê 4+ scenario gây duplicate trong Kafka
- [ ] Implement Inbox Pattern với consumer_group + UNIQUE constraint
- [ ] Biết khi nào dùng Idempotency-Key (HTTP) vs Inbox (Kafka)
- [ ] Giải thích vì sao compensation cũng cần idempotent
- [ ] Xử lý out-of-order events (partition key, versioning)
- [ ] Hiểu giới hạn của Kafka EOS (không xuyên sang DB)
- [ ] Viết được skeleton chuẩn cho Kafka consumer với @Transactional + Inbox + Business + Outbox
- [ ] Liệt kê 10 bẫy phổ biến và cách tránh

---

## Tổng kết phần lý thuyết (Bài 1-6)

Bạn đã đi qua **6 lớp tri thức**:
```
ACID (local DB)
   ↓
CAP / Eventual Consistency (distributed)
   ↓
2PC: tại sao thất bại
   ↓
Saga: lời giải kiến trúc
   ↓
Outbox: giải Dual Write Problem
   ↓
Idempotency + Inbox: giải Duplicate Problem
```

→ Đây là **bộ tri thức nền** của Senior Backend / Solution Architect trong domain microservice transaction. Mọi pattern khác (Event Sourcing, CQRS, Choreography Choreography hybrid, Process Manager...) đều build trên nền này.

---

**Bài tiếp theo:** `07 - Setup môi trường & Project structure (Spring Boot multi-module + Kafka + PostgreSQL + Docker Compose)` — bắt đầu vào phần code thực sự của demo.
