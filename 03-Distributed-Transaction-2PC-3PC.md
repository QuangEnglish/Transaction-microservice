# Distributed Transaction: 2PC, 3PC và lý do thất bại trong microservices

> Bài 3 trong lộ trình Transaction Microservice nâng cao.
> Mục tiêu: Hiểu các giao thức distributed transaction "cổ điển" (2PC, 3PC, XA), tại sao chúng tồn tại, và **tại sao industry hiện đại đã từ bỏ chúng** để chuyển sang Saga.

---

## 0. Nhắc lại context

- **Bài 1:** ACID đảm bảo transaction nguyên tử trong 1 DB.
- **Bài 2:** Trong distributed system, CAP buộc chọn AP hoặc CP. Microservices chọn AP → Eventual Consistency.
- **Câu hỏi bài 3:** Nhưng nếu **bắt buộc** phải có ACID xuyên nhiều DB/service thì sao? Có cách nào không?

→ Câu trả lời lịch sử: **2PC (Two-Phase Commit)**. Câu trả lời hiện tại: **Đừng dùng nó. Dùng Saga.**

Bài này giải thích **tại sao**.

---

## 1. Distributed Transaction là gì

> Distributed Transaction = một transaction trải dài qua **nhiều resource manager** (RM), thường là nhiều database, nhiều service, hoặc database + message queue.

### Ví dụ:
```
Transaction T:
  - Trừ tiền tài khoản A trong DB1
  - Cộng tiền tài khoản B trong DB2
  - Ghi log audit vào DB3
  - Publish event "TransferCompleted" vào Kafka
```

Tất cả 4 thao tác phải **all-or-nothing**: hoặc cả 4 thành công, hoặc cả 4 rollback.

→ Vấn đề: 4 hệ thống khác nhau, mỗi cái có transaction riêng của nó. **Ai điều phối?**

---

## 2. Two-Phase Commit (2PC)

Giao thức kinh điển, do Jim Gray đề xuất 1978. Là nền tảng của chuẩn **XA (X/Open XA)**.

### 2.1. Các vai trò

- **Transaction Coordinator (TC):** điều phối viên trung tâm
- **Participants (RM - Resource Managers):** các DB/service tham gia transaction

### 2.2. Hai phase

**Phase 1: PREPARE (Voting)**
1. TC gửi `PREPARE` đến tất cả participants
2. Mỗi participant:
   - Thực hiện thao tác local (UPDATE, INSERT...) nhưng **chưa commit**
   - Ghi vào WAL (write-ahead log) trạng thái "prepared"
   - Lock các row liên quan
   - Trả lời `YES` (sẵn sàng commit) hoặc `NO` (không thể)
3. TC chờ tất cả vote

**Phase 2: COMMIT / ABORT (Decision)**
- Nếu **tất cả** vote `YES` → TC gửi `COMMIT` đến tất cả
- Nếu **bất kỳ** vote `NO` (hoặc timeout) → TC gửi `ABORT` đến tất cả
- Participants thực hiện commit/rollback local và ack lại TC

### 2.3. Ví dụ minh họa

```
                  TC                  RM1               RM2
                   |                    |                 |
   Phase 1:        |---- PREPARE ------>|                 |
                   |---- PREPARE -------|---------------->|
                   |<-- YES ------------|                 |
                   |<-- YES ----------------------------- |
                   |                    |                 |
   Phase 2:        |---- COMMIT ------->|                 |
                   |---- COMMIT --------|---------------->|
                   |<-- ACK ------------|                 |
                   |<-- ACK ----------------------------- |
                   |                    |                 |
                  Done.
```

### 2.4. Đặc tính
- **Atomicity:** đảm bảo
- **Consistency:** đảm bảo (nếu tất cả RM đều ACID)
- **Isolation:** đảm bảo (lock được giữ trong suốt 2 phase)
- **Durability:** đảm bảo (mỗi RM tự đảm bảo)

→ Nhìn có vẻ hoàn hảo. Vậy vấn đề ở đâu?

---

## 3. Vì sao 2PC thất bại trong thực tế

### 3.1. Blocking khi Coordinator chết

Đây là vấn đề **trí mạng** nhất.

Kịch bản: TC đã nhận `YES` từ tất cả, đang chuẩn bị gửi `COMMIT` thì **TC crash**.

Các RM đang ở trạng thái "prepared":
- Đã lock data
- Đã ghi WAL "prepared"
- **Không biết** kết quả cuối: commit hay abort?
- → **Không thể quyết định một mình**, vì có thể TC đã gửi COMMIT cho RM khác rồi
- → **Block vĩnh viễn**, giữ lock vô thời hạn

Đây gọi là **"in-doubt transaction"**. Trong production XA, đây là cơn ác mộng của DBA.

### 3.2. Synchronous & Slow

- Mọi RM phải **chờ nhau**
- 2 round-trip network giữa TC và mỗi RM
- Latency = max(latency của RM chậm nhất) × 2
- Trong khi đó, **toàn bộ data đều bị lock**
- → Throughput thấp, contention cao

### 3.3. Tight Coupling

- Tất cả RM phải online cùng lúc
- Tất cả phải support XA protocol
- Một RM yếu = cả transaction yếu
- → **Vi phạm triết lý microservices** (loose coupling, independent deployment)

### 3.4. Không support nhiều resource hiện đại

XA hỗ trợ tốt cho RDBMS truyền thống. Nhưng:
- **Kafka**: không hỗ trợ XA chuẩn (có "exactly-once" nhưng cơ chế khác)
- **MongoDB, Cassandra, Redis**: không hỗ trợ XA
- **REST API**: hoàn toàn không có khái niệm prepare/commit
- **gRPC**: tương tự

→ Trong stack microservices điển hình (Spring Boot + Kafka + PostgreSQL + Redis), 2PC **không thể áp dụng**.

### 3.5. Khả năng mở rộng kém

- Càng nhiều RM → xác suất 1 RM fail càng cao → transaction càng dễ abort
- Với N participants, xác suất thành công ≈ (p)^N với p = xác suất 1 RM OK
- → 2PC chỉ thực tế khi N nhỏ (2-3)

### 3.6. Heuristic decisions = nightmare

Khi RM bị block quá lâu chờ TC, DBA có thể "heuristic commit" hoặc "heuristic abort" thủ công. Nhưng nếu quyết định sai (so với quyết định cuối cùng của TC), bạn có **inconsistent state** mà DBMS không phát hiện được.

---

## 4. Three-Phase Commit (3PC)

Sinh ra để **giải quyết blocking** của 2PC.

### 4.1. Ý tưởng

Thêm 1 phase trung gian gọi là **PRE-COMMIT** giữa PREPARE và COMMIT.

- Phase 1: **CAN-COMMIT?** (vote)
- Phase 2: **PRE-COMMIT** (nói "sắp commit rồi đấy")
- Phase 3: **DO-COMMIT** (commit thật)

Nếu TC chết giữa chừng, các RM có thể **timeout và tự quyết định** dựa trên trạng thái phase đang ở:
- Đang ở phase 1 → tự abort (an toàn)
- Đang ở phase 2 (đã PRE-COMMIT) → tự commit (vì biết các RM khác cũng đã ACK pre-commit)

### 4.2. Vì sao 3PC vẫn ít dùng

- **Giả định mạng không phân vùng**: 3PC hoạt động đúng chỉ khi không có network partition. Có partition → có thể inconsistent (vi phạm CAP).
- **Thêm 1 round-trip**: chậm hơn 2PC thêm ~50%
- **Phức tạp hơn nhiều**: code khó viết đúng, khó test
- **Implementation thực tế hiếm**: hầu hết DB không hỗ trợ 3PC sẵn

→ 3PC chủ yếu là kiến thức **lý thuyết**. Production hầu như không ai dùng.

---

## 5. XA Protocol & JTA — 2PC trong Java

### 5.1. XA là gì
XA = chuẩn của X/Open Group để chuẩn hoá interface giữa TC và RM. Định nghĩa các function: `xa_start`, `xa_prepare`, `xa_commit`, `xa_rollback`, `xa_recover`...

### 5.2. JTA (Java Transaction API)
Đặc tả Java cho distributed transaction. API chính: `UserTransaction`, `TransactionManager`.

### 5.3. Implementations phổ biến
- **Atomikos**, **Bitronix**, **Narayana** (JBoss): JTA standalone
- **WebLogic, WebSphere**: built-in JTA
- **Spring** có thể dùng JTA qua `JtaTransactionManager`

### 5.4. Code mẫu Spring + JTA

```java
@Configuration
public class JtaConfig {
    @Bean
    public JtaTransactionManager transactionManager() {
        return new JtaTransactionManager();
    }
}

@Service
public class TransferService {
    @Autowired DataSource db1;
    @Autowired DataSource db2;

    @Transactional  // JTA, xuyên DB
    public void transfer(Long from, Long to, BigDecimal amount) {
        // UPDATE trên db1 và db2 — JTA đảm bảo atomic
    }
}
```

→ **Code thì đẹp**, nhưng đằng sau là 2PC với tất cả nhược điểm đã liệt kê.

### 5.5. Khi nào còn nên dùng XA/JTA?
- **Monolith** với 2-3 database nội bộ
- **Legacy system** (banking core)
- Khi cả stack đều support XA và **không có** message broker / NoSQL
- Khi business **bắt buộc** strong consistency và không thể design lại

→ Trong stack microservices hiện đại: **gần như không bao giờ**.

---

## 6. So sánh tổng kết

| Tiêu chí | 2PC / XA | 3PC | Saga (sẽ học) |
|---|---|---|---|
| Atomicity | Có | Có (no partition) | Không (eventual) |
| Blocking | Có | Giảm | Không |
| Latency | Cao | Cao hơn | Thấp |
| Throughput | Thấp | Thấp | Cao |
| Network partition | Block | Có thể sai | Chịu được |
| Support Kafka, REST | Không | Không | Có |
| Loose coupling | Không | Không | Có |
| Phù hợp microservices | Không | Không | Có |
| Độ phức tạp code | Trung bình (lib lo) | Cao | Cao (mình tự lo) |
| Production maturity | Cao (40+ năm) | Thấp | Cao (10+ năm) |

---

## 7. Tại sao industry chuyển sang Saga

Tóm gọn lý do industry (Netflix, Amazon, Uber, Microsoft...) đã bỏ 2PC để dùng Saga:

1. **Microservices cần independent deployment** — 2PC tight-couple chúng lại
2. **Kafka, NoSQL, REST không hỗ trợ XA**
3. **Latency phải thấp** — 2PC quá chậm
4. **Availability quan trọng hơn consistency tức thời** — chấp nhận eventual
5. **Scale phải lên 1000+ service** — 2PC không scale
6. **Compensating logic** (rollback bằng business action) phù hợp với business domain hơn

→ Triết lý mới: **"Đừng chống lại tự nhiên của distributed system. Thiết kế hệ thống để chịu được sự không nhất quán tạm thời, rồi đảm bảo cuối cùng nhất quán."**

Đó chính là Saga.

---

## 8. Outbox & CDC — preview vai trò

Có một vấn đề con quan trọng: **làm sao "ghi DB + publish event" atomic?**

Ví dụ: OrderService cần
```
1. INSERT INTO orders ...
2. Publish event "OrderCreated" to Kafka
```

Nếu (1) OK mà (2) fail → DB có order nhưng event không phát → các service khác không biết → inconsistency.
Nếu (2) OK mà (1) fail → đã publish event nhưng DB không có → ma luôn.

→ Đây gọi là **Dual Write Problem**.
→ 2PC giữa DB và Kafka → không có (Kafka không support XA chuẩn).
→ Giải pháp: **Outbox Pattern** (ghi event vào cùng DB như 1 transaction, rồi 1 process khác đọc và publish). Đây là bài học sau.

---

## 9. Bẫy phổ biến

### Bẫy 1: "Microservice của tôi nhỏ, dùng 2PC cho nhanh"
Không. Một khi đã chia service, đừng kéo 2PC vào. Code design dựa vào 2PC sẽ rất khó migrate sau này.

### Bẫy 2: Lẫn lộn @Transactional với distributed transaction
`@Transactional` của Spring (mặc định) chỉ là **local transaction trên 1 DataSource**. Không phải distributed. Muốn distributed phải config `JtaTransactionManager`.

### Bẫy 3: Tưởng Kafka transaction = XA transaction
Kafka có **transactional producer** (`transactional.id`, `initTransactions()`, `beginTransaction()`, `commitTransaction()`), nhưng đây là transaction **trong phạm vi Kafka** (multiple topics/partitions), KHÔNG phải XA với DB.

### Bẫy 4: Tưởng "Saga = thay thế hoàn toàn 2PC"
Sai. Saga chỉ đảm bảo **eventual consistency** + **compensation**, KHÔNG đảm bảo isolation. Sẽ có giai đoạn "đang giữa flow" mà data ở các service inconsistent.
→ Cần design business logic và UI/UX cho phù hợp.

### Bẫy 5: Nghĩ rằng "không dùng XA = không cần học XA"
Senior/Architect cần hiểu XA để:
- Maintain legacy
- Tranh luận trade-off
- Hiểu sâu vì sao Saga ra đời

---

## 10. Gợi ý thực hành

1. Tạo Spring Boot project với 2 DataSource (2 PostgreSQL), config Atomikos, viết transfer money function. Kill app giữa chừng (sau prepare, trước commit) → xem trạng thái "in-doubt" trong PostgreSQL bằng query `SELECT * FROM pg_prepared_xacts`.
2. Manually resolve in-doubt transaction: `COMMIT PREPARED 'gid'` hoặc `ROLLBACK PREPARED 'gid'`.
3. Đọc paper gốc: **"Notes on Database Operating Systems"** của Jim Gray (1978) — phần nói về 2PC.
4. Đọc chương 9 "Consistency and Consensus" trong **Designing Data-Intensive Applications**, mục Atomic Commit & 2PC.
5. So sánh thực nghiệm: viết 2 phiên bản cùng business logic — 1 dùng JTA, 1 dùng Saga đơn giản. Bench latency và throughput.

---

## Checklist tự đánh giá

- [ ] Giải thích được Dual Write Problem
- [ ] Vẽ được sơ đồ 2 phase của 2PC, kèm các message giữa TC và RM
- [ ] Liệt kê 6 lý do 2PC thất bại trong microservices
- [ ] Giải thích "in-doubt transaction" và hậu quả
- [ ] Hiểu 3PC giải quyết gì, vì sao vẫn không phổ biến
- [ ] Phân biệt @Transactional thường, JTA, và Kafka transaction
- [ ] Trình bày được vì sao industry chuyển sang Saga
- [ ] Biết khi nào (hiếm) vẫn nên dùng XA/JTA

---

**Bài tiếp theo:** `04 - Saga Pattern: Orchestration vs Choreography` — vào chính thức world của microservice transaction.
