# CAP Theorem + BASE + Eventual Consistency — Bước chuyển sang thế giới phân tán

> Bài 2 trong lộ trình Transaction Microservice nâng cao.
> Mục tiêu: Hiểu vì sao ACID không scale được sang nhiều service/DB, và những "luật chơi" mới của hệ phân tán mà mọi Senior/Solution Architect phải nắm.

---

## 0. Nhắc lại context

Bài 1 kết luận: **ACID chỉ đúng trong phạm vi 1 database**.
Khi flow `Order → Inventory → Payment → Shipping` chạy trên 4 service với 4 DB riêng, ta không thể `BEGIN TRANSACTION` xuyên hệ thống.

Câu hỏi đặt ra: **Vậy hệ phân tán hoạt động theo nguyên tắc gì?**
→ Trả lời nằm ở 3 khái niệm: **CAP, PACELC, BASE**.

---

## 1. CAP Theorem

Do Eric Brewer đưa ra năm 2000, được Gilbert & Lynch chứng minh hình thức năm 2002.

> **Trong một hệ phân tán có chia mạng (network partition), bạn chỉ có thể đảm bảo 2 trong 3 thuộc tính: Consistency, Availability, Partition Tolerance.**

### 1.1. Định nghĩa chính xác từng chữ

**C - Consistency (Linearizability)**
Mọi node trong hệ thống đều thấy **cùng một dữ liệu tại cùng một thời điểm**. Sau khi write thành công ở node A, bất kỳ read nào tiếp theo (kể cả ở node B, C) đều phải trả về giá trị mới nhất.

> ⚠️ Đây **không phải** là Consistency trong ACID. CAP Consistency = Linearizability = "như thể chỉ có 1 bản copy duy nhất".

**A - Availability**
Mọi request gửi tới một node **không bị lỗi** đều nhận được response (không error, không timeout). Lưu ý: không yêu cầu response phải là dữ liệu mới nhất.

**P - Partition Tolerance**
Hệ thống vẫn tiếp tục hoạt động ngay cả khi mạng bị chia cắt — tức là một số message giữa các node bị mất hoặc delay vô thời hạn.

### 1.2. Hiểu đúng bản chất

Cách hiểu phổ biến "chọn 2 trong 3" là **sai lệch**. Cách hiểu đúng:

- **Network partition là chuyện sẽ xảy ra** trong mọi hệ phân tán thực tế (cáp đứt, switch hỏng, GC pause, network congestion). Bạn không có lựa chọn bỏ P.
- → Thực tế chỉ có 2 lựa chọn: **CP** (hy sinh A khi có partition) hoặc **AP** (hy sinh C khi có partition).

### 1.3. Ví dụ trực quan

Hệ có 2 node: Node1 (Hà Nội), Node2 (Sài Gòn), đồng bộ qua mạng. Mạng giữa 2 node bị đứt.

**Lựa chọn CP (ưu tiên Consistency):**
- Client ghi vào Node1 → Node1 không thể replicate sang Node2 → từ chối write (hoặc block)
- → Vẫn nhất quán, nhưng **mất availability**
- Ví dụ: Zookeeper, etcd, HBase, MongoDB (mặc định), PostgreSQL với synchronous replication

**Lựa chọn AP (ưu tiên Availability):**
- Client ghi vào Node1 → Node1 chấp nhận ngay, sẽ sync sau
- Client đọc từ Node2 → trả về giá trị cũ
- → Luôn trả lời, nhưng **dữ liệu có thể lệch tạm thời**
- Ví dụ: Cassandra, DynamoDB, Couchbase, Riak

### 1.4. Ai chọn gì?

| Hệ thống | Loại | Lý do |
|---|---|---|
| **Cassandra, DynamoDB** | AP | High availability, eventual consistency là OK với social feed, cart |
| **MongoDB (default)** | CP | Có primary node, partition → mất write |
| **HBase, BigTable** | CP | Strong consistency cho big data analytics |
| **Zookeeper, etcd, Consul** | CP | Coordination service phải nhất quán tuyệt đối |
| **Kafka** | CP (tuỳ config) | Với `acks=all` + `min.insync.replicas` |
| **Redis Cluster** | AP | Default; có thể tuning |
| **PostgreSQL (sync replication)** | CP | Master-slave |

---

## 2. PACELC — Mở rộng CAP cho đúng thực tế

CAP chỉ nói tới hành vi **khi có partition**. Nhưng 99% thời gian hệ thống **không** có partition — vậy lúc đó thế nào?

Daniel Abadi (2012) đưa ra **PACELC**:

> Nếu có **P**artition: chọn giữa **A**vailability hay **C**onsistency.
> **E**lse (bình thường): chọn giữa **L**atency hay **C**onsistency.

### Ví dụ phân loại theo PACELC

| Hệ thống | Khi partition | Khi bình thường |
|---|---|---|
| **DynamoDB** | PA | EL (ưu tiên low latency) |
| **Cassandra** | PA | EL |
| **MongoDB** | PC | EC |
| **HBase** | PC | EC |
| **PostgreSQL** | PC | EC |

→ PACELC quan trọng vì nó phản ánh **trade-off thực tế hàng ngày**: muốn read nhanh thì phải chấp nhận đọc replica có thể stale.

---

## 3. BASE — Triết lý của hệ AP

BASE là **đối lập** với ACID, áp dụng cho hệ phân tán ưu tiên availability:

### B - Basically Available
Hệ thống **luôn phản hồi** mọi request, kể cả khi response là dữ liệu cũ hoặc partial. Không có chuyện "service down".

### S - Soft State
Trạng thái hệ thống có thể **thay đổi theo thời gian** ngay cả khi không có input mới (do sync giữa các node). Khác với ACID — state chỉ đổi khi có transaction.

### E - Eventually Consistent
Sau một khoảng thời gian (không có write mới), tất cả các node **cuối cùng sẽ hội tụ** về cùng một giá trị.

### So sánh ACID vs BASE

| | ACID | BASE |
|---|---|---|
| Consistency | Strong (ngay lập tức) | Eventual (sau một lúc) |
| Availability | Có thể hy sinh để đảm bảo C | Ưu tiên tuyệt đối |
| Scale | Khó scale ngang | Scale ngang dễ |
| Use case | Banking, booking | Social feed, cart, recommendation |
| Phù hợp với | Monolith, RDBMS | Microservices, NoSQL |

---

## 4. Eventual Consistency — Đào sâu

Đây là khái niệm **quan trọng nhất** trong bài, vì nó là nền tảng để hiểu Saga + Outbox sau này.

### 4.1. Định nghĩa

> Nếu không có write mới nào nữa, thì sau một khoảng thời gian hữu hạn (convergence time), tất cả các bản copy của dữ liệu sẽ trở nên giống nhau.

Điểm yếu: **"sau một khoảng thời gian"** = bao lâu? 100ms? 5 giây? 5 phút? Tùy hệ thống và workload.

### 4.2. Các biến thể của Eventual Consistency

Không phải EC nào cũng giống nhau. Có **phổ** từ yếu đến mạnh:

**Read-your-writes consistency**
Sau khi user A ghi, **bản thân user A** sẽ luôn đọc được dữ liệu mới. Người khác thì chưa chắc.
→ Quan trọng cho UX: user post comment thì phải tự thấy comment của mình ngay lập tức.

**Monotonic reads**
Nếu đã đọc được version v, các lần đọc sau không bao giờ thấy version cũ hơn v.
→ Tránh "time travel": vừa thấy có comment, refresh lại thấy biến mất.

**Monotonic writes**
Các write của cùng 1 client được apply theo đúng thứ tự.
→ Ví dụ: update profile name, rồi update profile avatar — không thể avatar áp dụng trước name.

**Causal consistency**
Nếu event B "nhân quả" sau event A (B "happens-after" A), thì mọi node đều thấy A trước B.
→ Reply comment phải xuất hiện sau comment gốc.

**Strong eventual consistency**
EC + bonus: các replica nhận cùng tập update (dù khác thứ tự) sẽ hội tụ về cùng state mà **không cần coordination**.
→ Implement bằng **CRDT** (Conflict-free Replicated Data Type).

### 4.3. Ví dụ thực tế

**E-commerce cart (DynamoDB):**
User thêm sản phẩm vào cart trên app, rồi vào web → có thể chưa thấy ngay. 1-2 giây sau refresh → thấy. Đây là EC, chấp nhận được.

**Bank transfer:**
A chuyển tiền cho B. Nếu B đọc balance 5 giây sau vẫn chưa thấy → tệ. Đây là use case **không** chấp nhận EC, phải dùng Strong Consistency (hoặc Saga + business workaround).

**Order status:**
Order vừa tạo, user F5 trang detail. Nếu chưa thấy → có thể OK (show "Processing"). Đây là EC chấp nhận được.

---

## 5. Strong vs Eventual Consistency — Khi nào dùng cái nào?

### Strong Consistency (Linearizability)
- **Khi cần:** financial transaction, inventory (oversell), authentication, leader election
- **Cách đạt:** synchronous replication, distributed consensus (Paxos, Raft), 2PC
- **Cost:** latency cao, availability thấp khi có partition

### Eventual Consistency
- **Khi cần:** social feed, comment, like count, view count, recommendation, search index, analytics
- **Cách đạt:** async replication, gossip protocol, CRDT
- **Cost:** dữ liệu lệch tạm thời, code phức tạp hơn để handle inconsistency

### Tuning theo từng operation (Cassandra/DynamoDB style)

Cassandra cho phép tune **consistency level per query**:
- `ONE`: nhanh, ít nhất quán
- `QUORUM`: cân bằng (N/2 + 1 node ack)
- `ALL`: chậm, mạnh nhất

Công thức nổi tiếng: **R + W > N → Strong consistency**
- N = tổng replica
- W = số node ack write
- R = số node trả lời read
- VD: N=3, W=2, R=2 → 2+2 > 3 → đảm bảo read luôn thấy write mới nhất

---

## 6. Áp dụng vào lộ trình microservices của bạn

Quay lại flow: `Order → Inventory → Payment → Shipping`.

### Câu hỏi 1: Strong hay Eventual?
- **Strong:** Cần distributed transaction (2PC/XA) → slow, fragile, không scale
- **Eventual:** Mỗi service có DB riêng, sync qua event → fast, scale, nhưng phải handle inconsistency

→ **Microservices thực tế gần như luôn chọn Eventual Consistency.**

### Câu hỏi 2: Vậy ai bảo đảm "cuối cùng nhất quán"?

Đây chính là vai trò của các pattern sắp học:

| Vấn đề | Pattern giải quyết |
|---|---|
| Mỗi service phải thực hiện business logic + publish event atomically | **Outbox Pattern** |
| Cả flow nhiều bước phải hoàn thành hoặc rollback toàn bộ | **Saga Pattern** |
| Event bị consume nhiều lần (Kafka at-least-once) | **Idempotency** |
| Lỗi ở giữa flow phải undo bước trước | **Compensating Transaction** |
| Service tạm thời down → message lưu lại để retry | **Kafka durable log** |

### Câu hỏi 3: User trải nghiệm thế nào với EC?

Đây là điểm **Senior phải nghĩ ra trước khi code:**

- Order vừa tạo → user F5 trang list orders có thể chưa thấy → UI hiển thị optimistic update
- Payment chưa xác nhận → show status "Processing", **không** show "Success" ngay
- Inventory chưa trừ kịp → có rủi ro oversell → cần reservation pattern
- Email confirmation gửi sau khi cả flow xong, không phải sau bước Order

---

## 7. Bẫy phổ biến mà Senior phải biết

### Bẫy 1: Nhầm Consistency của ACID với Consistency của CAP
- ACID-C: ràng buộc dữ liệu (FK, unique...)
- CAP-C: linearizability — tất cả node thấy giống nhau cùng lúc
- → Trong phỏng vấn, nói rõ bạn đang nói về cái nào.

### Bẫy 2: "Chúng tôi chọn CA"
Trong distributed system, **không có lựa chọn này**. Partition là chuyện sẽ xảy ra. Ai nói "CA" là chưa hiểu CAP.
(Trừ phi nói về single-node DB như MySQL standalone — lúc đó không có distributed system.)

### Bẫy 3: Coi Eventual Consistency là "luôn luôn OK"
EC không miễn phí. Phải **thiết kế UI/UX** để user không bị bối rối. Phải handle các edge case (đọc stale, write conflict).

### Bẫy 4: Không định nghĩa SLA cho convergence
"Eventually consistent" không có nghĩa là "không cần biết bao lâu". Production cần đo: P50, P99 của replication lag.

### Bẫy 5: Dùng 2PC để "có ACID xuyên service"
2PC tồn tại nhưng:
- Block khi coordinator chết
- Latency cao (2 round-trip)
- Không support trên Kafka, REST
- Không scale ngang
→ Industry hầu như đã từ bỏ 2PC, thay bằng Saga.

---

## 8. Cầu nối tới bài tiếp theo

Bạn đã hiểu:
- **Tại sao** microservices không thể có ACID xuyên hệ thống → vì CAP buộc phải chọn
- **Microservices chọn AP** → Eventual Consistency là thực tế
- **Cần pattern** để đảm bảo "eventually" thật sự xảy ra

Bài tiếp theo: **Distributed Transaction — 2PC, 3PC và vì sao chúng thất bại trong microservices**. Đây là bước cuối trong phần "lý thuyết nền" trước khi bước vào Saga/Outbox.

---

## 9. Gợi ý thực hành

1. Đọc paper gốc: **"CAP Twelve Years Later"** của Eric Brewer (2012) — chỉ 6 trang, rất sáng.
2. Cài MongoDB replica set 3 node trên Docker, dùng `tc` (traffic control) hoặc `iptables` để mô phỏng partition, quan sát hành vi.
3. Cài Cassandra 3 node, thử các consistency level `ONE` / `QUORUM` / `ALL`, đo latency.
4. Đọc chương 5 (Replication), 8 (Trouble with Distributed Systems), 9 (Consistency and Consensus) trong **Designing Data-Intensive Applications**.
5. Tìm hiểu case study: Amazon DynamoDB paper (2007), Google Spanner paper (2012) — 2 thái cực AP và CP.

---

## Checklist tự đánh giá

- [ ] Định nghĩa chính xác C, A, P trong CAP (đặc biệt phân biệt với ACID-C)
- [ ] Giải thích vì sao "chọn 2 trong 3" là hiểu sai
- [ ] Cho ví dụ hệ CP, hệ AP và lý do chọn
- [ ] Phát biểu PACELC và cho ví dụ
- [ ] Thuộc BASE và so sánh với ACID
- [ ] Liệt kê và giải thích các biến thể của Eventual Consistency
- [ ] Giải thích công thức R + W > N
- [ ] Trình bày được vì sao microservices buộc phải chọn Eventual Consistency
- [ ] Liệt kê 5 bẫy phổ biến khi nói về CAP/EC

---

**Bài tiếp theo:** `03 - Distributed Transaction: 2PC, 3PC và lý do thất bại trong microservices`
