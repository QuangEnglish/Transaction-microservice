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

> ⚠️ Lưu ý: Consistency trong ACID **khác** Consistency trong CAP theorem (định lý: thia rừm). Trong ACID, đây là consistency về mặt ràng buộc dữ liệu (do bạn và DBMS định nghĩa).

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
→ Không còn bác sĩ nào trực! Invariant(bất biến: in ve ri ừn) bị phá vỡ.
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

### <mark>Read Uncommitted</mark>

Cho phép đọc dữ liệu chưa commit (Dirty Read).

```Transaction
@Transactional
public void transactionA() throws Exception {

    Account account = repo.findById(1L).get();

    account.setBalance(new BigDecimal("5000"));

    repo.save(account);

    System.out.println("A updated = 5000");

    Thread.sleep(15000);

    throw new RuntimeException("Rollback");
}
```

```Transaction
@Transactional(isolation = Isolation.READ_UNCOMMITTED)
public void transactionB() {

    Account account = repo.findById(1L).get();

    System.out.println("B read = " + account.getBalance());
}
```

> ## Kết quả
> 
> ```
> A updated = 5000B read = 5000
> ```
> 
> Sau đó:
> 
> ```
> Transaction A rollback
> ```

### <mark>Read Committed</mark> (mặc định của PostgreSQL, Oracle, SQL Server)

Chỉ đọc dữ liệu đã commit. Mỗi statement nhìn thấy một snapshot (ảnh chụp nhanh) mới nhất tại thời điểm statement(lời tuyên bố: sờ tây t mừn) bắt đầu.

```
@Transactional(isolation = Isolation.READ_COMMITTED)
public void transactionB() {

    Account account = repo.findById(1L).get();

    System.out.println("B read = " + account.getBalance());
}
```

> ## Kết quả
> 
> A:
> 
> ```
> update 5000(chưa commit)
> ```
> 
> B:
> 
> ```
> read = 1000
> ```
> 
> => Chỉ đọc dữ liệu đã commit.

### <mark>Repeatable Read</mark> (mặc định của MySQL InnoDB)

Trong cùng 1 transaction, đọc cùng row → cùng kết quả. Snapshot được lấy tại thời điểm transaction bắt đầu (statement đầu tiên).

```
Transaction A
@Transactional(isolation = Isolation.REPEATABLE_READ)
public void transactionA() throws Exception {

    Account account1 = repo.findById(1L).get();

    System.out.println("First read = " + account1.getBalance());

    Thread.sleep(10000);

    Account account2 = repo.findById(1L).get();

    System.out.println("Second read = " + account2.getBalance());
}


Transaction B
@Transactional
public void transactionB() {

    Account account = repo.findById(1L).get();

    account.setBalance(new BigDecimal("5000"));

    repo.save(account);
}
```

> Kết quả:
> 
> ```
> First read = 1000B update = 5000Second read = 1000
> ```
> 
> => Luôn nhìn thấy snapshot ban đầu.

> # So sánh
> 
> | MySQL InnoDB                                                                                          | SQL Server                     |
> | ----------------------------------------------------------------------------------------------------- | ------------------------------ |
> | Consistent Read: đọc từ snapshot, không khóa dữ liệu (SELECT)                                         | Snapshot Read / Row Versioning |
> | Current Read: Đọc phiên bản mới nhất, có khóa dữ liệu (UPDATE, DELETE, INSERT, SELECT ... FOR UPDATE) | Lock-based Read                |
> | MVCC                                                                                                  | Version Store trong tempdb     |
> | Read View                                                                                             | Transaction Snapshot           |
> | SELECT FOR UPDATE                                                                                     | UPDLOCK / XLOCK                |

### <mark> Serializable</mark> (cao nhất)

Như thể các transaction chạy tuần tự. PostgreSQL implement bằng **SSI (Serializable Snapshot Isolation)**: cho phép chạy song song nhưng detect conflict và abort 1 trong 2.

```
Transaction A
@Transactional(isolation = Isolation.SERIALIZABLE)
public void transactionA() throws Exception {

    long count1 = orderRepo.count();

    System.out.println("Count1 = " + count1);

    Thread.sleep(10000);

    long count2 = orderRepo.count();

    System.out.println("Count2 = " + count2);
}  


Transaction B
@Transactional
public void transactionB() {

    orderRepo.save(new Order(...));

    System.out.println("Insert success");
}
```

> ### Serializable
> 
> ```
> Count1 = 3
> 
> B waiting...
> 
> Count2 = 3
> 
> A commit
> 
> B insert success---
> ```

=> Database ép các transaction chạy tuần tự như: A hoàn thành  ↓  B mới được chạy

## 4. MVCC vs Locking — cách database thực sự làm

Đây là phần đa số dev bỏ qua nhưng cực quan trọng cho Senior.

### Lock-based (Pessimistic)

Trước khi đọc/ghi, transaction phải acquire lock (shared lock cho read, exclusive lock cho write). Cách cũ, gây contention cao, dễ deadlock.

- "Tao không tin ai cả, muốn đọc hay sửa gì thì phải khóa trước."

- Shared Lock (S Lock) - Đọc 
  
  ✅ Người khác vẫn được đọc
  
  ❌ Không ai được sửa

- Exclusive (ik sklu :siv) Lock (X Lock) - Ghi 
  
  ❌ Không ai đọc được
  
  ❌ Không ai sửa được
  
  Phải đợi A commit.

- ## Vì sao gọi là Pessimistic?
  
  "Pessimistic" = Bi quan.
  
  Database giả định:
  
  > "Có khả năng người khác sẽ sửa dữ liệu của tao."
  
  Nên:
  
  ```
  Khóa trước Làm việc sau
  ```

- Contention là sự tranh giành: vd 100 request cùng update vào 1 bản ghi thì request 1 update xong giữ lock các request sau sẽ phải đợi, đây gọi là Lock Contention

- Deadlock là 2 ông giữ lock của nhau, transaction A giữ lock , transaction B giữ lock, A tiếp tục phải chờ B, B tiếp tục phải chờ A.

### MVCC - Multi-Version Concurrency Control (Optimistic)

Mỗi row có nhiều **version**. Khi update, không ghi đè mà tạo version mới với transaction ID. Reader đọc version "nhìn thấy được" theo snapshot của mình → **reader không block writer, writer không block reader**.

PostgreSQL và MySQL InnoDB đều dùng MVCC. Đó là lý do PostgreSQL có `VACUUM` (dọn version cũ), MySQL có `undo log`.

> ## Tóm tắt cực dễ nhớ
> 
> ### Lock-based (SQL Server truyền thống)
> 
> ```
> Muốn đọc -> khóa
> Muốn ghi -> khóa
> 
> => An toàn
> => Dễ bị chờ
> => Dễ deadlock
> ```
> 
> ```
> Muốn đọc -> đọc snapshot
> 
> => Ít block
> => Hiệu năng cao
> => Ít deadlock hơn
> ```

### SELECT FOR UPDATE — pessimistic (bi quan: pesi'mistik) lock thủ công

```sql
BEGIN;
SELECT * FROM account WHERE id = 1 FOR UPDATE;  -- lock row này
-- xử lý business logic
UPDATE account SET balance = ... WHERE id = 1;
COMMIT;
```

Dùng khi cần đảm bảo không ai đụng vào row này cho đến khi mình commit. Cẩn thận **deadlock** nếu lock nhiều row theo thứ tự khác nhau.

- Pessimistic Lock là Tôi không tin transaction khác.

- Nếu không dùng Pessimistic Lock (select for update) vd: Tài khoản A có 1000, transaction 1 select tài khoản A có bao nhiêu tiền sau đó muốn trừ đi 300, transaction 2 chạy cùng lúc cũng đọc được tài khoản A còn 1000 rồi lại muốn trừ 500, kết quả T1 update 700, T2 update lại còn 500 và số dư cuối cùng là còn 500 lẽ ra phải là 200.

- Còn nếu dùng Pessimistic Lock (select for update) thì Transaction 1 đọc tài khoản còn bao nhiêu và sẽ lock lại rồi đi update lại số tiền bị trừ, xong transaction 1 commit thì lúc này mới chạy tiếp Transaction 2, đọc tk còn 700 và trừ đi 500 và kết quả cuối cùng còn 200, đúng.

### Optimistic Lock — version column

```sql
UPDATE account 
SET balance = 150, version = version + 1 
WHERE id = 1 AND version = 5;
```

Nếu affected rows = 0 → ai đó đã update trước → retry hoặc báo lỗi. JPA hỗ trợ qua `@Version`.

- Optimistic Lock là Cứ cho mọi người sửa thoải mái, lúc commit mới kiểm tra có ai sửa trước mình không

- VD: user A đọc bản ghi thấy có số tiền là 1000 và version là 5, user B cũng đọc bản ghi thấy có số tiền là 1000 và version là 5. User A update trước và số tiền giờ chỉ còn 700 và version được nâng lên 6, trong khi đó user B update sau vẫn nghĩ bản ghi đó version là 5 nên câu lệnh update:

- UPDATE account
  SET balance = 700,
  
      version = version + 1
  
  WHERE id = 1
    AND version = 5;    bị  sai => update không thành công

- JPA @Version hoạt động thế nào? Entity:
  
   @Entity
  public class Account {
  
      @Id
      private Long id;
      
      private BigDecimal balance;
      
      @Version
      private Long version;
  
  }

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

Đây là một trong những lỗi rất hay gặp với `@Transactional`, `@Cacheable`, `@Async`, `@Retryable`

Spring AOP hoạt động bằng **Proxy Pattern**.

- Spring không đưa `UserService` thật vào container mà tạo:

- Client
    ↓
  Proxy
    ↓
  UserService

- Khi gọi: userService.createUser(); thực tế là: proxy.createUser();

- Luồng thực tế vd trên sai là:

- Proxy
    ↓
  register()
    ↓
  this.createUser()

- Lúc này: this.createUser()  gọi trực tiếp vào object thật. **Không đi qua Proxy.**
  
  => `@Transactional` bị bỏ qua.

- Mong muốn là :
  
  register()
     ↓
  Proxy
     ↓
  createUser()
     ↓
  @Transactional hoạt động

- Thực tế:
  
  register()
     ↓
  this.createUser()
     ↓
  Không qua Proxy
     ↓
  @Transactional không hoạt động

- 

- 

- 

- Cách khắc phục:
  
  Cách 1: Tách class (khuyến nghị)
  
  Cách 2: Inject chính mình (self injection)
  
  @Service
  public class UserService {
  
      @Autowired
      private UserService self;
      
      public void register() {
          self.createUser();
      }
      
      @Transactional
      public void createUser() {
          // save db
      }
  
  }

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
