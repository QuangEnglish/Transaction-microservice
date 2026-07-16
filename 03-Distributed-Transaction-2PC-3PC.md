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

Two-Phase Commit (2PC) là một giao thức giúp **nhiều database hoặc nhiều service cùng thực hiện một transaction**.

Ý tưởng của nó rất đơn giản:

> **Hoặc tất cả cùng thành công, hoặc tất cả cùng thất bại. Không được phép có chuyện nơi này commit còn nơi khác rollback.**

Giao thức này được **Jim Gray** đề xuất vào năm **1978** và sau này trở thành nền tảng của chuẩn **XA Transaction**.

---

#### 2.1. Thành phần tham gia

Trong 2PC có 2 vai trò chính.

##### 1. Transaction Coordinator (TC)

Đây là **người điều phối**.

Nó không trực tiếp sửa dữ liệu mà chỉ có nhiệm vụ:

- hỏi mọi người đã sẵn sàng chưa,
- quyết định commit hay rollback,
- rồi thông báo quyết định đó cho tất cả.

Có thể hình dung Coordinator giống như **trọng tài** của trận đấu.

---

##### 2. Resource Manager (RM)

Đây là các database hoặc service tham gia transaction.

Ví dụ:

- Database của Order
- Database của Inventory
- Database của Payment

Mỗi RM chỉ chịu trách nhiệm xử lý dữ liệu của chính mình.

---

#### 2.2. Hai giai đoạn của 2PC

Đúng như tên gọi **Two-Phase Commit**, giao thức gồm **2 bước**.

---

##### Giai đoạn 1: PREPARE (Chuẩn bị)

Coordinator sẽ đi hỏi từng participant:

> **"Bạn đã sẵn sàng commit chưa?"**

Lúc này mỗi RM sẽ:

- thực hiện UPDATE hoặc INSERT,
- **chưa commit ngay**,
- ghi thông tin transaction vào **WAL (Write-Ahead Log)** để nếu hệ thống bị crash vẫn biết mình đang ở trạng thái nào,
- khóa (lock) các dòng dữ liệu liên quan,
- sau đó trả lời:

```
YES  -> Tôi đã sẵn sàng commit.
```

hoặc

```
NO -> Tôi không thể commit.
```

Coordinator sẽ **đợi tất cả RM phản hồi** trước khi quyết định bước tiếp theo.

---

##### Giai đoạn 2: COMMIT hoặc ABORT

Sau khi nhận đủ phản hồi, Coordinator sẽ đưa ra quyết định cuối cùng.

##### Trường hợp 1: Tất cả đều trả lời YES

Coordinator gửi lệnh:

```
COMMIT
```

đến tất cả RM.

Lúc này mỗi RM sẽ:

- commit transaction,
- ghi dữ liệu xuống database,
- giải phóng lock,
- gửi ACK để xác nhận đã hoàn thành.

---

##### Trường hợp 2: Chỉ cần một RM trả lời NO

Ví dụ:

```
RM1 -> YESRM2 -> NORM3 -> YES
```

Coordinator sẽ lập tức gửi:

```
ABORT
```

cho tất cả.

Kết quả:

- RM1 rollback
- RM2 rollback
- RM3 rollback

Không có bất kỳ database nào được phép commit.

Đó chính là nguyên tắc:

> **Hoặc tất cả cùng thành công, hoặc tất cả cùng thất bại.**

---

#### 2.4. Vì sao 2PC đảm bảo ACID?

##### 1. Atomicity (Tính nguyên tử)

Được đảm bảo rất tốt.

Hoặc tất cả cùng commit.

Hoặc tất cả cùng rollback.

Không tồn tại trạng thái "database này thành công nhưng database kia thất bại".

---

##### 2. Consistency (Tính nhất quán)

Nếu tất cả Resource Manager đều tuân thủ ACID thì sau transaction, dữ liệu trên toàn hệ thống vẫn nhất quán.

---

##### 3. Isolation (Tính cô lập)

Trong suốt thời gian chờ Coordinator quyết định, các RM **vẫn giữ lock** trên dữ liệu.

Điều này giúp transaction khác không thể sửa cùng dữ liệu, tránh xung đột.

---

##### 4. Durability (Tính bền vững)

Trước khi trả lời `YES`, mỗi RM đều ghi transaction vào **Write-Ahead Log (WAL)**.

Nhờ vậy, nếu hệ thống bị mất điện hoặc crash, RM vẫn có thể khôi phục trạng thái và tiếp tục xử lý sau khi khởi động lại.



**Vậy tại sao các hệ thống microservices hiện đại như Amazon, Netflix hay Uber lại gần như không sử dụng 2PC?**

#### 3. Vì sao 2PC thất bại trong thực tế

##### Nhìn qua, 2PC gần như hoàn hảo. Tuy nhiên khi triển khai trên hệ thống lớn, nó gặp rất nhiều hạn chế.

##### 3.1. Coordinator bị chết (Blocking)

Đây là nhược điểm lớn nhất.

Giả sử tất cả Resource Manager (RM) đều đã trả lời **YES**, nhưng trước khi gửi lệnh **COMMIT**, Coordinator lại bị crash.

Lúc này các RM:

- Đã cập nhật dữ liệu nhưng chưa commit.
- Đang giữ lock trên dữ liệu.
- Không biết nên **commit hay rollback**.
- Không thể tự quyết định vì có thể Coordinator đã gửi COMMIT cho RM khác.

→ Kết quả là transaction bị **kẹt (blocking)** và lock có thể bị giữ rất lâu.

---

##### 3.2. Chậm vì phải chờ nhau

Trong 2PC:

- Tất cả RM phải phản hồi trước khi tiếp tục.
- Coordinator phải trao đổi với mỗi RM qua **2 lần giao tiếp** (Prepare và Commit).
- Trong lúc chờ, dữ liệu vẫn bị lock.

→ Chỉ cần một RM chậm là cả transaction chậm theo.

---

##### 3.3. Các service phụ thuộc nhau

2PC yêu cầu:

- Tất cả service phải đang hoạt động.
- Tất cả đều phải hỗ trợ giao thức XA.
- Một service gặp sự cố có thể làm cả transaction thất bại.

→ Điều này đi ngược với mục tiêu của **Microservices**, nơi mỗi service nên hoạt động độc lập.

---

##### 3.4. Không hỗ trợ nhiều công nghệ hiện đại

2PC hoạt động tốt với các database quan hệ như Oracle hay PostgreSQL.

Nhưng nhiều thành phần phổ biến trong Microservices lại không hỗ trợ XA, ví dụ:

- Kafka
- Redis
- MongoDB
- REST API
- gRPC

→ Vì vậy rất khó áp dụng 2PC cho một hệ thống Microservices hiện đại.

---

##### 3.5. Khó mở rộng

Càng nhiều RM tham gia thì khả năng có một RM gặp lỗi càng cao.

Ví dụ:

- 2 RM → khá dễ thành công.
- 10 RM → chỉ cần 1 RM lỗi là cả transaction phải rollback.

→ Hệ thống càng lớn thì 2PC càng kém hiệu quả.

---

##### 3.6. Khó xử lý khi bị kẹt

Nếu transaction bị block quá lâu, quản trị viên đôi khi phải **tự quyết định** commit hoặc rollback.

Nếu quyết định sai, dữ liệu giữa các hệ thống có thể bị **không nhất quán**, và việc khôi phục sẽ rất phức tạp.

---

##### Kết luận

Nhược điểm lớn nhất của 2PC là:

- ❌ Dễ bị block nếu Coordinator gặp sự cố.
- ❌ Chậm vì mọi service phải chờ nhau.
- ❌ Các service phụ thuộc chặt chẽ vào nhau.
- ❌ Khó áp dụng với Kafka, Redis, REST API...
- ❌ Khả năng mở rộng kém.

Chính vì vậy, đa số hệ thống **Microservices hiện đại** không sử dụng 2PC mà chuyển sang các mô hình như **Saga Pattern**, **Event-Driven Architecture** và **Eventual Consistency**.

---

## 4. Three-Phase Commit (3PC)

3PC ra đời để **khắc phục nhược điểm bị block của 2PC**.

#### 4.1. Ý tưởng

Thay vì chỉ có **2 bước**, 3PC thêm một bước ở giữa gọi là **PRE-COMMIT**.

Quy trình gồm 3 giai đoạn:

1. **CAN-COMMIT?** → Hỏi các RM có sẵn sàng không.
2. **PRE-COMMIT** → Thông báo: *"Mọi thứ đã ổn, chuẩn bị commit."*
3. **DO-COMMIT** → Thực hiện commit thật.

Điểm khác biệt là nếu **Coordinator bị crash**, các RM có thể **chờ một khoảng thời gian rồi tự quyết định**:

- Đang ở **CAN-COMMIT** → tự **Rollback**.
- Đang ở **PRE-COMMIT** → tự **Commit**.

→ Nhờ đó giảm được tình trạng transaction bị kẹt như trong 2PC.

---

#### 4.2. Vì sao 3PC vẫn ít được sử dụng?

Mặc dù cải thiện được 2PC, nhưng 3PC vẫn có nhiều hạn chế:

- Phải **trao đổi thêm 1 bước**, nên chậm hơn 2PC.
- Hoạt động tốt khi **mạng ổn định**, nếu xảy ra **network partition** vẫn có thể làm dữ liệu không nhất quán.
- Cài đặt phức tạp và khó kiểm thử.
- Hầu hết database hiện nay **không hỗ trợ 3PC**.

---

#### Kết luận

3PC giải quyết được một phần vấn đề **blocking** của 2PC, nhưng đổi lại hệ thống trở nên **phức tạp và chậm hơn**.

---

## 5. XA Protocol & JTA — 2PC trong Java

#### 5.1. XA là gì?

**XA** là một chuẩn giúp nhiều database hoặc nhiều hệ thống **tham gia cùng một transaction**.

Nó định nghĩa cách **Coordinator** giao tiếp với các **Resource Manager**, ví dụ:

- Bắt đầu transaction.
- Chuẩn bị commit.
- Commit.
- Rollback.
- Khôi phục khi có sự cố.

Có thể hiểu đơn giản:

> **XA chính là "luật chơi" của giao thức 2PC.**

---

#### 5.2. JTA là gì?

**JTA (Java Transaction API)** là API của Java dùng để làm việc với **distributed transaction**.

Thay vì tự viết logic 2PC, lập trình viên chỉ cần đánh dấu:

```
@Transactional
```

Phần còn lại sẽ do JTA xử lý.

---

#### 5.3. Một số framework phổ biến

Java có nhiều thư viện hỗ trợ JTA như:

- Atomikos
- Bitronix
- Narayana (JBoss)

Ngoài ra, Spring cũng hỗ trợ thông qua **JtaTransactionManager**.

---

#### 5.4. Ví dụ trong Spring

```
@Transactional
public void transfer(...) {    // Cập nhật DB1
    // Cập nhật DB2
}
```

Nhìn vào code thì rất đơn giản.

Nhưng bên dưới, JTA vẫn đang sử dụng **Two-Phase Commit (2PC)** để đảm bảo cả hai database cùng **commit hoặc rollback**.

---

#### 5.5. Khi nào nên dùng XA/JTA?

XA/JTA vẫn phù hợp nếu:

- Ứng dụng **Monolith**.
- Chỉ có **2–3 database** nội bộ.
- Toàn bộ hệ thống đều hỗ trợ XA.
- Doanh nghiệp bắt buộc phải có **Strong Consistency**.

---

#### Kết luận

XA và JTA giúp việc lập trình **đơn giản hơn**, nhưng **không loại bỏ được các nhược điểm của 2PC** như:

- Dễ bị block.
- Hiệu năng thấp.
- Khó mở rộng.
- Không phù hợp với Kafka, Redis, NoSQL hay kiến trúc Microservices.

Vì vậy, trong các hệ thống **Microservices hiện đại**, **XA/JTA rất hiếm khi được sử dụng**; thay vào đó người ta ưu tiên **Saga Pattern** và **Event-Driven Architecture**.

---

## 6. So sánh tổng kết

| Tiêu chí              | 2PC / XA            | 3PC               | Saga (sẽ học)    |
| --------------------- | ------------------- | ----------------- | ---------------- |
| Atomicity             | Có                  | Có (no partition) | Không (eventual) |
| Blocking              | Có                  | Giảm              | Không            |
| Latency               | Cao                 | Cao hơn           | Thấp             |
| Throughput            | Thấp                | Thấp              | Cao              |
| Network partition     | Block               | Có thể sai        | Chịu được        |
| Support Kafka, REST   | Không               | Không             | Có               |
| Loose coupling        | Không               | Không             | Có               |
| Phù hợp microservices | Không               | Không             | Có               |
| Độ phức tạp code      | Trung bình (lib lo) | Cao               | Cao (mình tự lo) |
| Production maturity   | Cao (40+ năm)       | Thấp              | Cao (10+ năm)    |

---

## 7. Tại sao industry chuyển sang Saga

Sau nhiều năm sử dụng 2PC, các công ty như **Netflix, Amazon, Uber, Microsoft...** dần chuyển sang **Saga Pattern** vì phù hợp hơn với kiến trúc Microservices.

#### 1. Microservices cần hoạt động độc lập

2PC yêu cầu tất cả service cùng tham gia một transaction, khiến chúng phụ thuộc lẫn nhau.

Saga cho phép mỗi service xử lý độc lập và giao tiếp qua event.

---

#### 2. Không phải công nghệ nào cũng hỗ trợ 2PC

Hệ thống hiện đại thường dùng:

- Kafka
- Redis
- MongoDB
- REST API

Phần lớn đều **không hỗ trợ XA**, nên rất khó áp dụng 2PC.

---

#### 3. Hiệu năng tốt hơn

2PC bắt các service phải chờ nhau và giữ lock trong suốt transaction.

Saga không cần lock toàn hệ thống nên xử lý nhanh và dễ mở rộng hơn.

---

#### 4. Chấp nhận Eventual Consistency

Thay vì yêu cầu dữ liệu phải nhất quán ngay lập tức, Saga chấp nhận dữ liệu **không nhất quán trong thời gian ngắn** và sẽ đồng bộ sau.

---

#### 5. Rollback theo nghiệp vụ

Khi có lỗi:

- **2PC:** Rollback transaction.
- **Saga:** Thực hiện **Compensating Action** (ví dụ: hủy đơn hàng, hoàn kho, hoàn tiền).

Điều này phù hợp với các bài toán nghiệp vụ thực tế hơn.

---

## 8. Outbox & CDC — preview vai trò

Sau khi bỏ 2PC, một câu hỏi mới xuất hiện:

> **Làm sao vừa ghi dữ liệu vào Database, vừa gửi Event lên Kafka mà không bị lệch dữ liệu?**

Ví dụ, khi tạo đơn hàng:

```
1. Lưu Order vào Database.2. Publish sự kiện OrderCreated lên Kafka.
```

Nếu:

- Database lưu thành công nhưng **publish Event thất bại** → các service khác không biết có đơn hàng mới.
- Publish Event thành công nhưng **Database lưu thất bại** → các service nhận được một sự kiện không tồn tại.

→ Đây được gọi là **Dual Write Problem**.

Vì **Kafka không hỗ trợ XA/2PC**, nên không thể dùng transaction để đảm bảo cả hai bước luôn thành công cùng lúc.

#### Giải pháp

Thay vì ghi vào **Database** và **Kafka** cùng lúc, ta sẽ:

- Ghi dữ liệu và Event vào **cùng một Database trong một transaction**.
- Sau đó, một tiến trình khác sẽ đọc Event và gửi lên Kafka.

Đó chính là **Outbox Pattern**, kết hợp với **CDC (Change Data Capture)** để đồng bộ dữ liệu một cách an toàn.

---

## 9. Bẫy phổ biến

#### Bẫy 1: "Microservice nhỏ thì dùng 2PC cho nhanh"

Đừng làm vậy.

Một khi đã chọn **Microservices**, hãy tránh phụ thuộc vào 2PC. Sau này mở rộng hoặc tách service sẽ rất khó.

---

#### Bẫy 2: Nhầm `@Transactional` là Distributed Transaction

`@Transactional` mặc định của Spring chỉ quản lý **transaction trên một Database**.

Muốn transaction giữa nhiều Database phải cấu hình **JTA/XA**.

---

#### Bẫy 3: Nhầm Kafka Transaction với XA

Kafka có **Transaction**, nhưng chỉ đảm bảo dữ liệu **bên trong Kafka**.

Nó **không thể** đảm bảo transaction giữa **Database và Kafka** như XA.

---

#### Bẫy 4: Nghĩ Saga thay thế hoàn toàn 2PC

Saga **không đảm bảo Strong Consistency**.

Nó chỉ đảm bảo **Eventual Consistency**, nên sẽ có lúc dữ liệu giữa các service chưa đồng bộ.

→ Business và UI cần được thiết kế để chấp nhận điều này.

---

#### Bẫy 5: Không dùng XA thì không cần học XA

Dù ít dùng trong hệ thống mới, XA vẫn rất quan trọng để:

- Hiểu các hệ thống cũ (Legacy).
- So sánh ưu, nhược điểm với Saga.
- Hiểu vì sao ngành phần mềm chuyển từ **2PC** sang **Saga Pattern**.

---

#### Kết luận

Hiểu **XA và 2PC** không phải để dùng trong mọi dự án, mà để biết **khi nào nên dùng và khi nào không nên dùng**, từ đó lựa chọn kiến trúc phù hợp cho từng bài toán.

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
