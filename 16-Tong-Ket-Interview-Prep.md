# Tổng kết & Interview Prep cho Senior/Solution Architect

> Bài 16 — bài tổng kết toàn lộ trình + chuẩn bị phỏng vấn Senior Backend / Solution Architect.
> Mục tiêu: Hệ thống hóa lại 13 bài, top câu phỏng vấn kèm answer mẫu, cách present demo, và lộ trình tiếp theo lên Architect.

---

## Phần 1: Tổng kết — Bạn đã học gì

### 1.1. Bản đồ kiến thức

```
NỀN TẢNG ────────────────────────────────────────
│
├── ACID (1 DB)
│   ├─ Atomicity, Consistency, Isolation, Durability
│   ├─ 5 hiện tượng concurrency
│   ├─ 4 isolation levels
│   └─ MVCC vs Locking
│
├── CAP / BASE (distributed)
│   ├─ CP vs AP, PACELC
│   ├─ Eventual consistency variants
│   └─ R + W > N
│
└── Lý do 2PC thất bại
    ├─ Blocking khi coordinator chết
    ├─ Kafka/NoSQL không support XA
    └─ Tight coupling

PATTERNS ───────────────────────────────────────
│
├── Saga
│   ├─ Choreography (event-driven)
│   ├─ Orchestration (centralized)
│   └─ Compensating transactions
│
├── Outbox
│   ├─ Giải Dual Write Problem
│   ├─ Polling vs CDC (Debezium)
│   └─ At-least-once delivery
│
└── Idempotency
    ├─ Inbox pattern
    ├─ Idempotency-Key
    ├─ State-based check
    └─ DB UNIQUE constraint

IMPLEMENTATION ────────────────────────────────
│
├── 5 services: Order, Inventory, Payment, Shipping, Saga Orchestrator
├── Stack: Spring Boot 3 + Kafka + PostgreSQL + Docker Compose
├── State machine với 10 states + transitions
└── Reservation pattern, Payment Gateway integration

QUALITY ──────────────────────────────────────
│
├── Testing: Unit + Integration + E2E (Testcontainers + Awaitility)
├── Failure Injection: Toxiproxy, Docker chaos, Game Day
└── Observability: OpenTelemetry + Jaeger + Prometheus + Grafana
```

### 1.2. Bạn đang ở đâu

| Skill | Mức trước lộ trình | Mức sau lộ trình |
|---|---|---|
| ACID / Isolation | Hiểu khái niệm | Giải thích MVCC, dùng tốt @Transactional |
| Distributed Transaction | Biết tên 2PC | Hiểu sâu vì sao 2PC thất bại, dùng Saga |
| Event-driven | Có dùng Kafka | Hiểu Dual Write, Outbox, Idempotency |
| Microservices | Build được service | Build được system với 5 service, chịu được failure |
| Testing | Unit test cơ bản | Testcontainers, E2E, chaos test |
| Observability | Log file | Distributed tracing, custom metrics, alerting |
| **Tổng** | **Mid-level Backend** | **Senior Backend (mạnh về DT)** |

Để lên **Solution Architect**, cần thêm (sẽ nói ở mục cuối):
- Architecture decision records (ADR)
- Non-functional requirements (capacity, security, cost)
- Multi-region, disaster recovery
- Team-level concerns (Conway's law, service ownership)
- Business context và technology strategy

---

## Phần 2: Top câu phỏng vấn kèm câu trả lời mẫu

### Nhóm A: Lý thuyết nền (Junior → Senior)

#### Câu 1: "Giải thích ACID. Cho ví dụ thực tế cho mỗi tính chất."

**Trả lời mẫu (2-3 phút):**

ACID là 4 thuộc tính của database transaction:

- **Atomicity (nguyên tử)**: hoặc tất cả thao tác commit, hoặc tất cả rollback. Ví dụ chuyển khoản A→B: trừ A và cộng B phải đi cùng. Cơ chế dưới capo: Undo Log / WAL.
- **Consistency (nhất quán)**: trước và sau transaction, DB phải valid theo mọi ràng buộc (FK, unique, check, business rule).
- **Isolation (cô lập)**: concurrent transactions không nhìn thấy state dở dang của nhau. SQL standard có 4 mức: Read Uncommitted → Read Committed → Repeatable Read → Serializable. Mỗi mức chặn thêm hiện tượng: dirty read, non-repeatable, phantom, write skew.
- **Durability (bền vững)**: sau commit, dữ liệu sống sót crash. Cơ chế: WAL fsync xuống đĩa trước khi báo commit.

**Lưu ý quan trọng**: Consistency trong ACID **khác** Consistency trong CAP. ACID-C là ràng buộc dữ liệu, CAP-C là linearizability.

---

#### Câu 2: "5 hiện tượng concurrency và mức isolation chặn cái nào?"

**Trả lời mẫu:**

5 hiện tượng:
1. **Dirty Read**: đọc dữ liệu chưa commit
2. **Non-repeatable Read**: cùng query trong 1 transaction trả 2 kết quả khác
3. **Phantom Read**: cùng range query trả thêm/bớt row
4. **Lost Update**: 2 transaction cùng đọc rồi cùng update → 1 update mất
5. **Write Skew**: 2 transaction đọc cùng dataset, mỗi cái update row khác, cộng lại phá invariant

Bảng chặn:
- Read Committed (default PostgreSQL): chặn dirty
- Repeatable Read (default MySQL InnoDB): chặn dirty + non-repeatable
- Serializable: chặn tất cả

**Note**: MySQL Repeatable Read còn chặn phantom nhờ gap lock. PostgreSQL Repeatable Read (Snapshot Isolation) chặn lost update bằng serialization_failure.

---

#### Câu 3: "Phân biệt Optimistic và Pessimistic Locking?"

**Trả lời mẫu:**

- **Pessimistic**: giả định conflict sẽ xảy ra → khóa trước. SQL: `SELECT ... FOR UPDATE`. JPA: `@Lock(LockModeType.PESSIMISTIC_WRITE)`. Phù hợp khi contention cao, conflict thường xảy ra. Risk: deadlock, throughput thấp.

- **Optimistic**: giả định conflict hiếm → không khóa, chỉ check khi update. Dùng version column. JPA: `@Version`. Update: `UPDATE ... WHERE version = ?`. Nếu affected rows = 0 → conflict → retry. Phù hợp web app, contention thấp.

Trong demo của em: dùng Optimistic cho Inventory (concurrent reserve), kèm Spring Retry để auto-retry khi `OptimisticLockingFailureException`.

---

### Nhóm B: Distributed System (Senior)

#### Câu 4: "Giải thích CAP theorem. Hệ thống của bạn thuộc loại nào?"

**Trả lời mẫu:**

CAP nói: trong distributed system có network partition, bạn chỉ có thể đảm bảo 2 trong 3 — Consistency, Availability, Partition tolerance.

Nhưng cách hiểu chính xác hơn: **Partition là chuyện sẽ xảy ra**, nên thực tế là chọn giữa **CP** (hy sinh availability khi có partition) và **AP** (hy sinh consistency tức thì).

Hệ thống của em (e-commerce saga) là **AP** với eventual consistency:
- Order vừa tạo → status PENDING → user thấy "đang xử lý"
- Saga xử lý async qua Kafka
- Cuối cùng (vài giây sau) → status CONFIRMED hoặc CANCELLED

Em chọn AP vì:
- Microservices buộc phải eventual consistency (4 DB riêng)
- 2PC không scale + Kafka không support XA
- User experience chấp nhận được nếu UI design tốt (show pending state, polling/push update)

Nâng cấp: PACELC — khi không có partition, vẫn phải chọn giữa Latency và Consistency. Hệ thống em ưu tiên L (low latency) qua eventual consistency.

---

#### Câu 5: "Tại sao không dùng 2PC giữa các service?"

**Trả lời mẫu (đây là câu phỏng vấn classic):**

5 lý do chính:

1. **Blocking khi coordinator chết**: nếu coordinator crash sau PREPARE, các participant đang ở "in-doubt" state — đã lock data, đã ghi WAL "prepared", không biết COMMIT hay ABORT, block vô thời hạn. Production XA = cơn ác mộng của DBA.

2. **Kafka và NoSQL không support XA**: stack microservices điển hình (Kafka + PostgreSQL + Redis) không thể setup 2PC. REST API hoàn toàn không có khái niệm prepare/commit.

3. **Latency cao**: 2 round-trip giữa coordinator và mỗi participant, lock giữ trong toàn bộ thời gian → throughput thấp.

4. **Tight coupling**: tất cả participant phải online cùng lúc, vi phạm triết lý loose coupling của microservices.

5. **Không scale**: xác suất thành công ≈ p^N với N participants. Càng nhiều service, càng dễ fail.

Industry đã chuyển sang **Saga** — chuỗi local ACID transaction + compensating transactions. Đánh đổi: mất isolation tức thì, được scalability và availability.

---

#### Câu 6: "Saga là gì? Phân biệt Orchestration và Choreography?"

**Trả lời mẫu:**

Saga là chuỗi local transactions, mỗi cái update 1 service. Nếu bước nào fail, chạy compensating transactions để undo các bước trước.

Compensating transaction **khác** rollback:
- Rollback (ACID): xoá vết, như chưa từng có
- Compensation: thêm hành động ngược lại, để lại lịch sử (audit trail)

Ví dụ: Payment fail → không "unsave" reservation, mà gọi `releaseStock()` — 1 transaction mới.

Hai cách điều phối:

**Choreography**: không có coordinator. Mỗi service listen events, publish events khi xong.
- Ưu: loose coupling, không single point of failure
- Nhược: logic flow phân tán → khó debug, dễ cyclic dependency
- Phù hợp: flow đơn giản, ≤ 4 bước

**Orchestration**: có Saga Orchestrator trung tâm, gửi command, nhận reply, manage state machine.
- Ưu: logic tập trung, dễ trace, compensation rõ ràng
- Nhược: single point of failure (cần HA), risk "God service"
- Phù hợp: flow phức tạp, nhiều nhánh

Trong demo em chọn Orchestration vì flow Order→Inventory→Payment→Shipping có 4 bước với compensation phức tạp. Hệ thống lớn thường hybrid: Orchestration cho core flow, Choreography cho side effects (notification, analytics).

---

#### Câu 7: "Compensation có khó không? Khi nào không thể compensate?"

**Trả lời mẫu (câu này phân biệt Senior và Mid-level):**

Compensation kỹ thuật không khó nhưng có 3 vấn đề Senior phải biết:

1. **Phải idempotent**: compensation có thể được gọi nhiều lần (Kafka at-least-once). Em design mỗi compensation kiểm tra state trước: `if (reservation.status == RELEASED) return;`.

2. **Phải theo đúng thứ tự ngược**: Saga forward đi `Inventory → Payment → Shipping`, compensation đi `Shipping → Payment → Inventory`. Sai thứ tự có thể leak state.

3. **Một số hành động không thể "undo" thuần**:
   - Email đã gửi → không thể "ungửi" → phải gửi email apology
   - Payment đã capture → có thể refund (compensation), nhưng tốn phí và thời gian
   - Hàng đã ship → recall vật lý, có khi không khả thi

→ Senior design phải:
- Đặt các bước "không undo được" càng muộn càng tốt trong flow
- Tách "soft commit" và "hard commit" (như em đã làm với Shipping: CreateShipment vs ConfirmShipment)
- Khi compensation fail → escalate (alert ops, không silent fail)

→ Đôi khi business buộc chấp nhận inconsistency tạm thời + manual reconciliation.

---

### Nhóm C: Outbox & Idempotency (Senior+)

#### Câu 8: "Bạn save Order vào DB rồi send event Kafka. Nếu Kafka send fail thì sao?"

**Trả lời mẫu (câu này test hiểu sâu về Dual Write):**

Đây là **Dual Write Problem** — và không giải được bằng cách sắp xếp code khéo léo:

- Save DB trước, Kafka sau → DB OK, Kafka fail → event mất → downstream không biết
- Kafka trước, DB sau → Kafka OK, DB fail → event ma
- Try-catch + manual rollback → network timeout không phân biệt được Kafka đã nhận hay chưa (Two Generals Problem)
- 2PC giữa DB và Kafka → Kafka không support XA chuẩn

**Lời giải: Outbox Pattern.**

Thay vì save DB + send Kafka, ta save DB + save "event vào outbox table" (cùng DB, cùng transaction → ACID đảm bảo atomicity). Sau đó 1 process riêng đọc outbox và publish sang Kafka.

```sql
@Transactional {
  INSERT INTO orders ...
  INSERT INTO outbox_events (event_type, payload, processed_at=NULL)
}
```

Sau đó:
- **Polling Publisher**: scheduled job query `WHERE processed_at IS NULL`, publish, mark processed. Dùng `FOR UPDATE SKIP LOCKED` để multi-instance.
- **CDC (Debezium)**: đọc WAL của Postgres real-time, stream sang Kafka. Latency thấp hơn, scale tốt hơn, infra phức tạp hơn.

Outbox đảm bảo **at-least-once delivery**. Consumer phải idempotent.

---

#### Câu 9: "At-least-once nghĩa là có thể duplicate. Làm sao xử lý?"

**Trả lời mẫu:**

Có 5 kỹ thuật, em thường kết hợp:

1. **Inbox Pattern**: bảng `inbox_events(message_id, consumer_group)` với UNIQUE constraint. Mỗi consumer ghi inbox trước business logic, cùng transaction. Duplicate message → INSERT inbox fail → skip.

2. **Idempotency-Key (HTTP)**: client gửi `Idempotency-Key` header, server cache response 24h+. Pattern industry standard cho payment API (Stripe, Adyen).

3. **Natural idempotency**: dùng UNIQUE constraint trên natural key (order_id). DB tự chặn duplicate.

4. **UPSERT / Conditional Update**: `INSERT ... ON CONFLICT DO UPDATE` hoặc `UPDATE ... WHERE version = ?`.

5. **State-based check**: trước khi action, check state. `if (order.status == CONFIRMED) return;`.

Trong demo, em dùng **3 tầng bảo vệ**:
- Inbox (chống duplicate Kafka message)
- State check (chống business duplicate)
- DB UNIQUE constraint (chống race condition giữa 2 thread)

**Quan trọng**: Idempotency phải theo **message identity** (message_id/event_id), không phải business identity (order_id). Vì cùng order có thể có nhiều event hợp lệ (Created, Updated, Cancelled).

---

#### Câu 10: "Kafka có exactly-once không? Có cần Inbox nữa không?"

**Trả lời mẫu (câu bẫy):**

Kafka từ 0.11+ có "exactly-once semantics" (EOS) với:
- Idempotent producer (`enable.idempotence=true`)
- Transactional producer (`transactional.id`)
- Read-process-write trong Kafka transaction

**NHƯNG**: Kafka EOS chỉ exactly-once **trong phạm vi Kafka** (giữa các topic). Khi consumer ghi xuống **DB ngoài Kafka** (PostgreSQL, MongoDB), Kafka EOS **không bao trùm**.

Trong stack microservices điển hình (Kafka + Postgres), Inbox vẫn cần thiết.

Kafka EOS hữu ích chủ yếu cho **Kafka Streams** — stream processing với input và output đều là Kafka topic.

→ Em vẫn dùng Inbox trong demo, không tin Kafka EOS che hết.

---

### Nhóm D: Architecture & Trade-offs (Senior → Architect)

#### Câu 11: "Design một e-commerce checkout flow đảm bảo no oversell, no double charge."

**Trả lời mẫu (system design):**

**Requirements:**
- Functional: customer add cart → checkout → pay → ship
- Non-functional: no oversell stock, no double charge card, high availability, scale 1000 orders/min

**High-level design:**
```
[Customer] ─► [API Gateway] ─► [Order Service]
                                   │
                              [Saga Orchestrator]
                              ┌────┼────┬─────┬─────┐
                              ▼    ▼    ▼     ▼     ▼
                          Inventory Payment Ship   Order
                            │       │      │      │
                          Postgres (4 DBs)
                              ▲
                          Kafka (events + commands)
```

**Key patterns:**

1. **Reservation pattern (Inventory)**: reserve stock at checkout (decrement `available`, increment `reserved`). Không trừ hẳn cho đến khi payment OK. TTL 15 phút auto-release.

2. **Saga orchestration**: 4 bước Order → Inventory → Payment → Shipping với compensation chain.

3. **Outbox** ở mỗi service: đảm bảo "ghi DB + publish event" atomic.

4. **Inbox** ở mỗi consumer: idempotent, chống Kafka redeliver.

5. **Payment gateway idempotency-key**: deterministic key = `paymentId + sagaId`, gửi gateway, gateway tự chống duplicate charge.

6. **Optimistic locking** trên inventory.available (`@Version`): chống race khi nhiều order cùng reserve hot product.

**UI/UX cho eventual consistency:**
- POST /orders → 202 Accepted (không phải 201)
- Show status PENDING với spinner
- Poll GET /orders/{id} hoặc WebSocket push khi xong
- Cancel button gửi explicit cancel command

**Failure handling:**
- Insufficient stock → Saga fail → order CANCELLED, no compensation needed
- Payment fail → release stock, cancel order
- Shipping fail → refund payment, release stock, cancel order
- Timeout → synthetic fail reply → trigger compensation chain

**Observability:**
- Trace mỗi saga end-to-end (Jaeger)
- Metrics: success rate, P99 duration, failure breakdown
- Alerts: failure rate > 5%, outbox lag > 100, saga stuck > 10 min

**Trade-offs em đã chọn:**
- AP over CP (eventual consistency) → scale tốt, UX cần design care
- Orchestration over Choreography → flow phức tạp, dễ trace
- Polling Outbox over CDC → đơn giản trước, migrate Debezium khi scale > 10k/s

---

#### Câu 12: "Nếu Kafka down 5 phút giữa lúc Saga đang chạy, hệ thống thế nào?"

**Trả lời mẫu:**

Tốt là em đã test scenario này (Game Day):

**Phía Producer (service đang publish event):**
- Business logic + outbox insert vẫn OK (Kafka không nằm trong transaction)
- OutboxRelay polling sẽ fail khi publish → `retry_count` tăng, exponential backoff
- Events tích trong `outbox_events` table

**Phía Consumer (service đang chờ event/reply):**
- Kafka consumer reconnect tự động (Spring Kafka handle)
- Không có message để process → idle

**Khi Kafka up trở lại:**
- OutboxRelay resume → publish backlogged events theo thứ tự `created_at`
- Consumers resume → consume từ last committed offset

**Saga in-flight:**
- Saga state đã persisted trong DB → không bị mất
- Nếu Saga đang chờ reply từ Inventory (ví dụ), `SagaTimeoutWatcher` sẽ fire nếu Kafka down > step timeout
- Synthetic fail reply → compensation trigger

**Monitor:**
- Outbox queue size tăng → alert
- Kafka consumer lag tăng → alert
- Saga stuck count tăng → alert

**Long outage (> max retries):**
- `outbox.retry_count >= 5` → stuck events
- Cần manual intervention: reset retry_count + investigate root cause

→ **Hệ thống degrade gracefully, không có data loss.** Em đã verify bằng cách `docker stop kafka` 5 phút, bắn 100 orders, restart Kafka, đo 100/100 orders eventually CONFIRMED, 0 data loss.

---

#### Câu 13: "Làm sao monitor 1 saga đang stuck?"

**Trả lời mẫu:**

3 tầng monitoring:

**Tầng 1: Logs (per-saga drill down)**
- Mỗi log line có `traceId, sagaId, orderId` (MDC)
- Query: `service:"saga-orchestrator" AND sagaId:"abc-123"` trong Loki/ELK
- Xem step nào fail, exception nào

**Tầng 2: Tracing (visual flow)**
- Jaeger UI: search by `saga.id=abc-123`
- Waterfall view: thấy span nào dài, span nào lỗi
- Span attributes: `saga.current_state`, `saga.is_compensating`

**Tầng 3: Metrics (aggregated trend)**
- Grafana dashboard: `saga_active_count` by state
- Stuck saga: count of `state != COMPLETED && updated_at < now() - 10m`
- Alert: `SagaStuck` fires khi count > 5

**Workflow debug:**
1. Alert fire → on-call mở dashboard
2. Identify pattern: stuck ở state nào? service nào down?
3. Pick 1 sagaId → Jaeger → tìm root cause
4. Fix: restart service, manual replay command, hoặc force fail saga + compensation

**API hỗ trợ debug:**
- `GET /api/sagas/by-order/{orderId}` → trả saga + steps history
- `GET /api/sagas/active` → list sagas non-terminal
- `POST /admin/sagas/{id}/force-fail` → manual abort + trigger compensation (last resort)

---

#### Câu 14: "Database per service vs shared database, trade-off?"

**Trả lời mẫu:**

**Shared DB (anti-pattern cho microservices):**
- Ưu: dễ join across service, không cần eventual consistency, ACID xuyên service
- Nhược:
  - Tight coupling — đổi schema 1 service phá service khác
  - Không scale độc lập
  - Không deploy độc lập (migrate schema = downtime cả service)
  - Vi phạm bounded context

**Database per service (đúng):**
- Mỗi service own data của nó. Service khác chỉ access qua API/event.
- Ưu:
  - Loose coupling
  - Scale độc lập, deploy độc lập
  - Tự do chọn DB phù hợp (Postgres cho transactional, Cassandra cho time-series, Redis cho cache)
- Nhược:
  - Mất ACID xuyên service → cần Saga
  - Mất join → cần data duplication hoặc API call
  - Eventual consistency → cần UI/UX design care

→ **Microservices đúng nghĩa = database per service.** Nếu share DB, đó là "distributed monolith" — tệ hơn cả monolith.

Trong demo em có 5 DB (order, inventory, payment, shipping, saga) — đúng pattern.

---

### Nhóm E: Behavioral & Soft Skills

#### Câu 15: "Kể về 1 lần bạn debug bug production khó."

**Trả lời theo STAR:**

**Situation**: Production e-commerce, alert "Saga failure rate spike 15%" lúc 3 AM. Em on-call.

**Task**: Tìm root cause + mitigate, target < 30 phút.

**Action**:
1. Mở Grafana → thấy failure reason chính: "Payment Gateway Timeout"
2. Drill down: P99 latency của payment gateway từ 200ms → 8s
3. Pick 1 sagaId fail, mở Jaeger → confirm payment span 8000ms với error
4. Check gateway provider status page → confirm họ đang có issue
5. Mitigation: tăng timeout từ 5s → 15s (config reload, no restart)
6. Long-term action item: implement circuit breaker với fallback gateway

**Result**: Failure rate giảm về < 1% sau 5 phút mitigation. Tổng MTTR (mean time to recovery) 25 phút. Tạo runbook cho gateway timeout. Tuần sau implement circuit breaker.

**Lesson**: Có observability tốt = debug nhanh. Trước đây 1 incident tương tự mất 3h.

→ **Template STAR** này áp dụng cho mọi behavioral question. Luôn có metric cụ thể, learning rõ ràng.

---

## Phần 3: Cách present demo trong phỏng vấn

### 3.1. Script 10-15 phút

**Mở (1 phút):**
> "Em build 1 hệ thống e-commerce với Saga pattern để giải distributed transaction problem. Tech stack: Spring Boot, Kafka, PostgreSQL. Em show 3 thứ: (1) architecture, (2) happy path live demo, (3) chaos demo — kill 1 service và watch recovery."

**Architecture (3 phút):**
- Vẽ ra slide hoặc Excalidraw: 5 services, Kafka, 5 DB
- Highlight: Outbox table mỗi service, Inbox table mỗi service, Saga Orchestrator state machine
- Nói trade-off đã chọn: Orchestration over Choreography, Polling Outbox over CDC

**Happy path (3 phút):**
- Show terminal split: 5 service logs + jaeger UI
- `curl POST /api/orders` 
- Switch sang Jaeger → show trace với 200+ spans cross 5 services
- Verify trong DB: order=CONFIRMED, payment=COMPLETED, inventory=committed, shipment=CREATED

**Chaos demo (4 phút):**
- Bắt đầu 1 saga
- `docker kill saga-orchestrator` giữa flow
- Show Grafana: saga active count tăng (stuck)
- `docker start saga-orchestrator`
- Watch dashboard: saga resume → COMPLETED
- Verify: 0 data loss, no duplicate

**Failure scenario (2 phút):**
- Tạo order với stock không đủ
- Watch saga flow: STARTED → AWAITING_STOCK → AWAITING_ORDER_CANCEL → FAILED
- Verify compensation: order=CANCELLED, no payment, no shipment

**Đóng (2 phút):**
> "Em đã test 9 chaos scenarios và verify invariant `orders.count == outbox.count` luôn đúng. Test coverage: unit 90%+, integration đầy đủ, E2E với Testcontainers. Em có thể đào sâu bất kỳ phần nào: state machine design, idempotency strategy, monitoring, hoặc trade-offs."

### 3.2. Câu mà interviewer thường hỏi follow-up

| Câu hỏi | Answer hint |
|---|---|
| "Vì sao chọn Orchestration?" | Flow 4 bước phức tạp, dễ trace, compensation rõ ràng. Hybrid khi cần. |
| "Outbox latency 500ms có acceptable không?" | Cho demo: yes. Production scale: migrate Debezium. Demo design đã chuẩn bị. |
| "Tại sao không dùng Temporal/Camunda?" | Để hiểu sâu cơ chế. Production có thể migrate. |
| "Nếu user mua hot product, 10k order/s cho 1 SKU, sao chịu được?" | Optimistic lock + retry → contention cao. Cần inventory partition hoặc reservation pre-allocation. |
| "Schema event evolve thế nào?" | Versioned events, backward-compatible Avro/Protobuf, consumer support multi version, deprecated old version sau N tháng. |

---

## Phần 4: 10 Câu mà 90% người fail

Đây là những "câu bẫy" rất hay được hỏi:

1. **"Consistency trong ACID và CAP khác nhau ở đâu?"**
   - Sai: trả lời giống nhau
   - Đúng: ACID-C = ràng buộc dữ liệu; CAP-C = linearizability

2. **"`@Transactional` của Spring có support distributed transaction không?"**
   - Sai: "Có, mặc định"
   - Đúng: Mặc định KHÔNG. Chỉ local transaction trên 1 DataSource. Distributed cần `JtaTransactionManager` + XA.

3. **"`@TransactionalEventListener(AFTER_COMMIT)` có thay được Outbox không?"**
   - Sai: "Có"
   - Đúng: KHÔNG. App crash giữa commit và listener → event mất. Phải persisted outbox.

4. **"Kafka exactly-once có giải Dual Write không?"**
   - Sai: "Có"
   - Đúng: Chỉ EOS giữa Kafka topics. Không cover Kafka ↔ DB external.

5. **"Saga có Isolation không?"**
   - Sai: "Có"
   - Đúng: KHÔNG. Giữa các bước, data ở các service inconsistent. Cần design UI/UX và business semantics (Reserved stock, Pending order).

6. **"Compensation có phải rollback không?"**
   - Sai: "Phải"
   - Đúng: Compensation là transaction MỚI thực hiện hành động ngược lại. Có audit trail.

7. **"Idempotency theo order_id có đúng không?"**
   - Sai: "Đúng"
   - Đúng: Sai. Cùng order có thể có nhiều event hợp lệ. Phải theo message_id/event_id.

8. **"At-least-once + Idempotent = Exactly-once không?"**
   - Sai: "Không"
   - Đúng: CÓ. Đây là công thức cho "effectively exactly-once" trong distributed system.

9. **"2 service cùng update inventory, optimistic lock fail thì sao?"**
   - Sai: "Throw exception, fail user"
   - Đúng: Auto-retry với backoff (Spring Retry). Chỉ throw cho user sau N lần retry.

10. **"Saga timeout, retry hay fail?"**
    - Sai: "Fail luôn"
    - Đúng: Tùy strategy. Production thường retry với backoff trước, fail sau N lần. Demo em chọn fail ngay cho đơn giản, mention được rằng có thể nâng cấp.

---

## Phần 5: Resources tiếp theo

### Sách (theo thứ tự đọc)

1. **"Designing Data-Intensive Applications" - Martin Kleppmann** (BẮT BUỘC)
   - Bible của distributed system
   - Đọc chương 7 (Transactions), 9 (Consistency), 11 (Stream Processing)

2. **"Microservices Patterns" - Chris Richardson**
   - Bible của microservice patterns
   - Đọc chương 4 (Saga), 6 (Outbox), 12 (Deploying microservices)

3. **"Building Microservices" - Sam Newman** (2nd edition)
   - Big picture, architecture decisions
   - Phù hợp khi đã hiểu pattern, muốn lên Architect

4. **"Release It!" - Michael Nygard**
   - Resilience patterns (circuit breaker, bulkhead, timeout)
   - Production war stories

5. **"Chaos Engineering" - Casey Rosenthal & Nora Jones**
   - Cho Game Day, advanced chaos

### Papers (đọc trực tiếp, free)

1. **"Sagas" (1987)** - Garcia-Molina & Salem. 9 trang, paper gốc.
2. **"CAP Twelve Years Later" (2012)** - Eric Brewer. 6 trang, làm rõ CAP.
3. **"Notes on Database Operating Systems" (1978)** - Jim Gray. Phần 2PC.
4. **"Amazon Dynamo" (2007)** và **"Google Spanner" (2012)** — 2 thái cực AP vs CP.
5. **"Kafka: a Distributed Messaging System for Log Processing" (2011)** - LinkedIn.

### Courses

- **Confluent Developer** (free): Apache Kafka course
- **Educative.io - Grokking the System Design Interview**
- **MIT 6.824 Distributed Systems** (free trên YouTube) — academic, depth cao

### Blogs / Talks

- **microservices.io** - Chris Richardson
- **Confluent Blog** - Kafka deep dive
- **High Scalability** blog
- **InfoQ** queue (architecture talks)
- **Netflix Tech Blog** - chaos engineering, resilience

---

## Phần 6: Lộ trình lên Solution Architect

Bạn đã có nền tảng **Senior Backend mạnh về DT**. Để lên Architect cần thêm:

### 6.1. Mở rộng tech depth

- **Event Sourcing + CQRS** — natural next step sau Saga
- **DDD (Domain-Driven Design)** — bounded context, aggregate, ubiquitous language
- **API Design** — REST maturity, GraphQL, gRPC, AsyncAPI
- **Multi-region / Geo-distribution** — DynamoDB Global Tables, CockroachDB
- **Disaster Recovery** — RTO, RPO, backup strategies
- **Security** — OAuth2/OIDC, mTLS, secrets management, PII handling

### 6.2. Build breadth

- Hiểu **Kubernetes** (deploy, scale, service mesh)
- Hiểu **Cloud** (AWS/GCP/Azure) — VPC, IAM, managed services
- Hiểu **Data platforms** — data lake, warehouse, streaming
- Hiểu **Frontend** đủ để discuss BFF, SSR, mobile concerns

### 6.3. Soft skill cấp Architect

- **Architecture Decision Records (ADR)** — viết được lý do mỗi quyết định
- **Non-functional requirements** — capacity planning, cost estimation
- **Conway's Law** — design system phù hợp team structure
- **Tech radar** — biết khi nào adopt tech mới vs ổn định
- **Mentorship** — guide team junior/mid level
- **Stakeholder communication** — present cho non-tech audience

### 6.4. Build portfolio

- 1 system demo (giống cái đã build)
- 1 deep-dive talk public (meetup, conference) hoặc blog series
- 1 open source contribution (Spring, Kafka, hoặc tool nhỏ của bạn)
- LinkedIn + GitHub active

### 6.5. Roadmap 18 tháng

```
Tháng 1-3: Refine demo này, deploy lên cloud (AWS), viết blog series 5 bài
Tháng 4-6: Học Event Sourcing + CQRS, extend demo
Tháng 7-9: Học Kubernetes + service mesh, deploy demo trên K8s
Tháng 10-12: Học DDD, refactor domain model demo
Tháng 13-15: Multi-region setup, chaos engineering production-grade
Tháng 16-18: Public speaking, mentor junior, apply position Architect
```

---

## Phần 7: Lời kết

Lộ trình 16 bài này dạy bạn **cách suy nghĩ của Senior/Architect**, không chỉ "biết viết code".

Sau khi hoàn thành:

✅ **Hiểu sâu** về distributed transaction từ A đến Z
✅ **Code được** demo production-grade với 5 services
✅ **Vận hành được** với observability + chaos engineering
✅ **Trả lời được** mọi câu phỏng vấn Senior về DT
✅ **Sẵn sàng** present demo trong phỏng vấn Solution Architect

**Điều quan trọng nhất:**

Đừng dừng lại ở "đọc xong tài liệu". Hãy **code thật**, **break thật**, **debug thật**. Mỗi pattern chỉ thực sự hiểu sau khi bạn implement nó, gặp bug, fix bug, và rút ra bài học.

Distributed system là một **subject for life** — luôn có điều mới để học. Lộ trình này là nền móng vững chắc. Từ đây, bạn tự đi tiếp được.

---

> *"Senior engineers don't just write code that works. They write code that works AND fails gracefully AND can be debugged at 3 AM AND scales 10x next year."*

Chúc bạn thành công trên hành trình lên Solution Architect. 🚀

---

**📚 Toàn bộ lộ trình (16 bài):**

1. ACID + Isolation Level
2. CAP + BASE + Eventual Consistency
3. Distributed Transaction: 2PC, 3PC
4. Saga Pattern: Orchestration vs Choreography
5. Outbox Pattern
6. Idempotency + Exactly-Once + Inbox
7. Setup Project Structure
8. Order Service Implementation
9. Inventory + Payment + Shipping Services
10. Saga Orchestrator + State Machine
11. End-to-End Testing với Testcontainers
12. Failure Injection & Chaos Engineering
13. Observability: Tracing + Metrics + Alerts
14. (Bonus) Debezium CDC — chưa làm, để tự nghiên cứu
15. (Bonus) Choreography variant — chưa làm, để tự nghiên cứu
16. Tổng kết & Interview Prep ← bài này

**Câu cuối cho bạn suy ngẫm:**

> *Người Senior trả lời câu hỏi "làm sao?". Người Architect trả lời câu hỏi "tại sao cách này, không phải cách khác?". Lộ trình này cho bạn nền tảng để trả lời cả hai.*
