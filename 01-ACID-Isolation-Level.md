# ACID + Isolation Level — Nền tảng Transaction

> Bài 1 trong lộ trình Transaction Microservice nâng cao.
> Mục tiêu: Hiểu sâu ACID và Isolation Level ở local database — điều kiện tiên quyết để hiểu **tại sao distributed transaction lại khó**, và **tại sao phải đẻ ra Saga/Outbox**.

---

## 1. ACID là gì

ACID là 4 thuộc tính mà một database transaction phải đảm bảo:

### A - Atomicity (Tính nguyên tử)

Transaction là "all or nothing". Hoặc tất cả các thao tác đều thành công (commit), hoặc tất cả đều bị hủy (rollback). Không có trạng thái nửa vời.

```sql
BEGIN;
UPDATE account SET balance = balance - 100 WHERE id = 1;  -- A trừ 100
UPDATE account SET balance = balance + 100 WHERE id = 2;  -- B cộng 100
COMMIT;
```

Nếu câu thứ 2 fail → câu 1 cũng bị rollback. Không bao giờ có chuyện "A bị trừ tiền nhưng B không nhận được".

**Cơ chế dưới capo:** Database dùng **Undo Log / Write-Ahead Log (WAL)** để có thể rollback.

### C - Consistency (Tính nhất quán)

Trước và sau transaction, database phải ở trạng thái **valid** theo tất cả ràng buộc đã định nghĩa: foreign key, unique, check constraint, trigger, business rule…

> ⚠️ Lưu ý: Consistency trong ACID **khác** Consistency trong CAP theorem. Trong ACID, đây là consistency về mặt ràng buộc dữ liệu (do bạn và DBMS định nghĩa).

### I - Isolation (Tính cô lập)

Khi nhiều transaction chạy đồng thời, kết quả phải **như thể chúng chạy tuần tự** (serial). Đây là phần phức tạp nhất, đào sâu ở mục Isolation Level bên dưới.

### D - Durability (Tính bền vững)

Sau khi commit thành công, dữ liệu phải tồn tại vĩnh viễn, kể cả khi server crash, mất điện…

**Cơ chế:** WAL (PostgreSQL), Redo Log (MySQL InnoDB) được fsync xuống đĩa trước khi báo commit thành công cho client.

---

## 2. Tại sao cần Isolation? Các hiện tượng concurrency

Khi nhiều transaction chạy song song mà không có isolation, có 5 hiện tượng "xấu xí" có thể xảy ra. Bạn **phải** thuộc lòng 5 cái này:

### 2.1. Dirty Read (Đọc dữ liệu chưa commit)

T1 update nhưng chưa commit. T2 đọc được giá trị đó. Sau đó T1 rollback → T2 đã đọc dữ liệu "ma".

```
T1: UPDATE account SET balance = 0 WHERE id = 1;  -- chưa commit
T2: SELECT balance FROM account WHERE id = 1;     -- đọc được 0
T1: ROLLBACK;                                      -- balance quay về cũ
                                                   -- nhưng T2 đã hành động dựa trên số 0
```

### 2.2. Non-repeatable Read (Đọc 2 lần khác nhau)

Trong cùng 1 transaction, T1 đọc cùng 1 row 2 lần, nhưng giá trị khác nhau vì T2 đã update + commit ở giữa.

```
T1: SELECT balance FROM account WHERE id = 1;  -- 100
T2: UPDATE account SET balance = 50 WHERE id = 1; COMMIT;
T1: SELECT balance FROM account WHERE id = 1;  -- 50 (khác lần đầu!)
```

### 2.3. Phantom Read (Đọc thấy "bóng ma")

Tương tự non-repeatable, nhưng với **tập kết quả** (range query). T1 query một dải, T2 INSERT thêm row vào dải đó, T1 query lại thấy nhiều row hơn.

```
T1: SELECT * FROM orders WHERE amount > 1000;  -- 5 rows
T2: INSERT INTO orders (..., amount) VALUES (..., 2000); COMMIT;
T1: SELECT * FROM orders WHERE amount > 1000;  -- 6 rows (xuất hiện 1 "phantom")
```

### 2.4. Lost Update (Mất update)

Hai transaction cùng đọc, cùng tính toán dựa trên giá trị đọc được, rồi cùng ghi đè → 1 update bị mất.

```
T1: SELECT balance FROM account WHERE id = 1;  -- đọc 100
T2: SELECT balance FROM account WHERE id = 1;  -- đọc 100
T1: UPDATE account SET balance = 100 + 50;     -- ghi 150
T2: UPDATE account SET balance = 100 + 30;     -- ghi 130 → mất 50 của T1!
```

### 2.5. Write Skew (Lệch ghi)

Tinh vi nhất. Hai transaction đọc cùng dataset, kiểm tra một invariant, rồi mỗi transaction ghi vào **những row khác nhau** sao cho riêng từng cái thì hợp lệ, nhưng cộng lại thì phá vỡ invariant.

Ví dụ kinh điển: ca trực bệnh viện phải có ít nhất 1 bác sĩ.

```
Hiện tại: bác sĩ A và B đều đang on-call.
T1 (A xin nghỉ): SELECT count(*) WHERE on_call=true;  -- 2, OK xin nghỉ
T2 (B xin nghỉ): SELECT count(*) WHERE on_call=true;  -- 2, OK xin nghỉ
T1: UPDATE doctor SET on_call=false WHERE id='A'; COMMIT;
T2: UPDATE doctor SET on_call=false WHERE id='B'; COMMIT;
→ Không còn bác sĩ nào trực! Invariant bị phá vỡ.
```

---

## 3. 4 Isolation Levels theo SQL standard

SQL standard định nghĩa 4 mức, mỗi mức **chặn** thêm một số hiện tượng. Càng cao càng an toàn nhưng càng chậm.

| Isolation Level      | Dirty Read    | Non-repeatable | Phantom | Lost Update | Write Skew |
| -------------------- | ------------- | -------------- | ------- | ----------- | ---------- |
| **Read Uncommitted** | Có thể xảy ra | Có thể         | Có thể  | Có thể      | Có thể     |
| **Read Committed**   | Chặn          | Có thể         | Có thể  | Có thể      | Có thể     |
| **Repeatable Read**  | Chặn          | Chặn           | Có thể* | Có thể**    | Có thể     |
| **Serializable**     | Chặn          | Chặn           | Chặn    | Chặn        | Chặn       |

\* MySQL InnoDB ở Repeatable Read đã chặn được phantom nhờ gap lock.
\*\* PostgreSQL ở Repeatable Read (thực chất là Snapshot Isolation) chặn được lost update bằng cách throw `serialization_failure`.

### Read Uncommitted

Hầu như không ai dùng. PostgreSQL còn không hỗ trợ thực sự (nó coi như Read Committed).

### Read Committed (mặc định của PostgreSQL, Oracle, SQL Server)

Chỉ đọc dữ liệu đã commit. Mỗi statement nhìn thấy một snapshot mới nhất tại thời điểm statement bắt đầu.

### Repeatable Read (mặc định của MySQL InnoDB)

Trong cùng 1 transaction, đọc cùng row → cùng kết quả. Snapshot được lấy tại thời điểm transaction bắt đầu (statement đầu tiên).

### Serializable (cao nhất)

Như thể các transaction chạy tuần tự. PostgreSQL implement bằng **SSI (Serializable Snapshot Isolation)**: cho phép chạy song song nhưng detect conflict và abort 1 trong 2.

---

## 4. MVCC vs Locking — cách database thực sự làm

Đây là phần đa số dev bỏ qua nhưng cực quan trọng cho Senior.

### Lock-based (Pessimistic)

Trước khi đọc/ghi, transaction phải acquire lock (shared lock cho read, exclusive lock cho write). Cách cũ, gây contention cao, dễ deadlock.

### MVCC - Multi-Version Concurrency Control (Optimistic)

Mỗi row có nhiều **version**. Khi update, không ghi đè mà tạo version mới với transaction ID. Reader đọc version "nhìn thấy được" theo snapshot của mình → **reader không block writer, writer không block reader**.

PostgreSQL và MySQL InnoDB đều dùng MVCC. Đó là lý do PostgreSQL có `VACUUM` (dọn version cũ), MySQL có `undo log`.

### SELECT FOR UPDATE — pessimistic lock thủ công

```sql
BEGIN;
SELECT * FROM account WHERE id = 1 FOR UPDATE;  -- lock row này
-- xử lý business logic
UPDATE account SET balance = ... WHERE id = 1;
COMMIT;
```

Dùng khi cần đảm bảo không ai đụng vào row này cho đến khi mình commit. Cẩn thận **deadlock** nếu lock nhiều row theo thứ tự khác nhau.

### Optimistic Lock — version column

```sql
UPDATE account 
SET balance = 150, version = version + 1 
WHERE id = 1 AND version = 5;
```

Nếu affected rows = 0 → ai đó đã update trước → retry hoặc báo lỗi. JPA hỗ trợ qua `@Version`.

---

## 5. Áp dụng trong Spring Boot

```java
@Service
public class OrderService {

    @Transactional(
        isolation = Isolation.READ_COMMITTED,
        propagation = Propagation.REQUIRED,
        timeout = 30,
        rollbackFor = Exception.class
    )
    public void placeOrder(OrderRequest req) {
        // các thao tác DB ở đây nằm trong 1 transaction
    }
}
```

**Các tham số quan trọng:**

- `isolation`: chọn 1 trong 4 mức trên (DEFAULT = lấy của DB)
- `propagation`: hành vi khi gọi method có @Transactional từ một transaction khác. Phổ biến:
  - `REQUIRED` (default): tham gia transaction hiện tại, không có thì tạo mới
  - `REQUIRES_NEW`: luôn tạo transaction mới, suspend cái cũ
  - `NESTED`: savepoint trong transaction cha
  - `MANDATORY`: phải có transaction sẵn, không thì throw
- `rollbackFor`: mặc định Spring chỉ rollback với `RuntimeException` và `Error`. Phải khai báo nếu muốn rollback với checked exception.

**Optimistic lock với JPA:**

```java
@Entity
public class Account {
    @Id Long id;
    BigDecimal balance;

    @Version
    Long version;  // JPA tự tăng và check
}
```

**Pessimistic lock với JPA:**

```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT a FROM Account a WHERE a.id = :id")
Account findByIdForUpdate(@Param("id") Long id);
```

---

## 6. Bẫy phổ biến mà Senior phải biết

### Bẫy 1: @Transactional bị bỏ qua khi gọi nội bộ

```java
public void methodA() {
    methodB();  // @Transactional ở B sẽ KHÔNG có hiệu lực!
}

@Transactional
public void methodB() { ... }
```

Vì Spring AOP dùng proxy, gọi nội bộ bypass proxy. Phải tách class hoặc inject `self`.

### Bẫy 2: Long transaction

Transaction càng dài → giữ lock càng lâu → contention, deadlock. Đừng gọi REST API, đừng send Kafka **bên trong** `@Transactional` — đây chính là lý do phải dùng **Outbox Pattern** (sẽ học sau).

### Bẫy 3: Isolation level không đủ cao

Nhiều case bug "ngẫu nhiên" trong production thực chất là race condition do dùng Read Committed mà không có optimistic/pessimistic lock.

### Bẫy 4: Deadlock

Hai transaction lock 2 row theo thứ tự ngược nhau. Giải pháp: luôn lock theo thứ tự cố định (ví dụ: theo id tăng dần).

---

## 7. Cầu nối tới microservices (preview)

Đến đây bạn có thể đặt câu hỏi quan trọng: **ACID chỉ đúng trong phạm vi 1 database**. Khi flow Order → Inventory → Payment → Shipping nằm trên **4 database khác nhau** (4 microservice), thì:

- Không thể `BEGIN TRANSACTION` xuyên 4 service
- 2PC (Two-Phase Commit) tồn tại nhưng chậm, dễ block, không scale, và Kafka/REST không hỗ trợ
- → Phải hy sinh **Strong Consistency** và chấp nhận **Eventual Consistency**
- → Đẻ ra các pattern: **Saga, Outbox, CDC, Idempotency, Compensating Transaction**

Đây chính là chỗ kiến thức ACID gặp giới hạn, và là lý do toàn bộ lộ trình của bạn tồn tại.

---

## 8. Gợi ý thực hành để chốt khái niệm này

1. Mở 2 tab `psql` (PostgreSQL) hoặc 2 session MySQL, tự tay tái hiện cả 5 hiện tượng (dirty read, non-repeatable, phantom, lost update, write skew).
2. Đổi `SET TRANSACTION ISOLATION LEVEL ...` và quan sát hành vi khác nhau.
3. Viết 1 endpoint Spring Boot mô phỏng "chuyển khoản 2 user" — thử với và không có `@Transactional`, thử dùng optimistic vs pessimistic lock, dùng JMeter bắn 100 request đồng thời để thấy race condition.
4. Đọc thêm: chương 7 "Transactions" trong sách **Designing Data-Intensive Applications** của Martin Kleppmann — bắt buộc cho ai muốn lên Solution Architect.

---

## Checklist tự đánh giá

- [ ] Giải thích được 4 thuộc tính ACID, kèm cơ chế dưới capo (WAL, Undo Log)
- [ ] Phân biệt được Consistency trong ACID vs trong CAP
- [ ] Vẽ được ví dụ cụ thể cho cả 5 hiện tượng concurrency
- [ ] Thuộc bảng 4 Isolation Level và cái nào chặn cái nào
- [ ] Hiểu MVCC khác Lock-based ở đâu, vì sao PostgreSQL cần VACUUM
- [ ] Biết khi nào dùng Optimistic vs Pessimistic Lock
- [ ] Liệt kê được 4 bẫy phổ biến với `@Transactional` trong Spring
- [ ] Giải thích được tại sao ACID không đủ trong microservices

---

**Bài tiếp theo:** `02 - CAP Theorem + BASE + Eventual Consistency` — bước chuyển từ "thế giới 1 DB" sang "thế giới phân tán".
