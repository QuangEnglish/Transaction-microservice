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

Trước khi tìm hiểu CAP, hãy tưởng tượng hệ thống của bạn không còn chạy trên **1 server** nữa mà được triển khai trên **nhiều server** ở nhiều nơi.

![](C:\Users\hquan\AppData\Roaming\marktext\images\2026-07-12-00-13-51-image.png)

Để đảm bảo nếu một server gặp sự cố thì hệ thống vẫn hoạt động, dữ liệu sẽ được **sao chép (Replication)** sang nhiều server.

Nhưng điều này lại sinh ra một câu hỏi rất lớn:

> **Nếu dữ liệu trên các server không giống nhau thì phải xử lý như thế nào?**

Đó chính là vấn đề mà **CAP Theorem** giải quyết.

##### CAP Theorem nói gì?

CAP Theorem được **Eric Brewer** đưa ra năm 2000 và được **Gilbert & Lynch** chứng minh vào năm 2002.

Nội dung của định lý là:

> **Khi hệ thống phân tán xảy ra sự cố mạng (Network Partition), bạn không thể vừa đảm bảo dữ liệu luôn đồng nhất (Consistency), vừa đảm bảo hệ thống luôn phản hồi (Availability). Bạn buộc phải chấp nhận đánh đổi một trong hai.**

Điều này khiến nhiều người hiểu thành:

> **"Chọn 2 trong 3 (C, A, P)"**

Thực ra cách hiểu này **chưa hoàn toàn chính xác**.

Lý do sẽ được giải thích ngay bên dưới.

##### Ba thành phần của CAP

##### <mark>C - Consistency (Tính nhất quán)</mark>

Hãy tưởng tượng bạn có **3 server** lưu cùng một dữ liệu.

![](C:\Users\hquan\AppData\Roaming\marktext\images\2026-07-12-00-16-42-image.png)

Bạn vừa chuyển khoản **10 triệu**.

Nếu sau khi giao dịch thành công, bạn đọc dữ liệu ở **bất kỳ server nào** cũng đều thấy số dư mới:

![](C:\Users\hquan\AppData\Roaming\marktext\images\2026-07-12-00-17-10-image.png)

=> Đây là **Consistency**.

Nói đơn giản:

> **Đọc ở server nào cũng nhìn thấy cùng một dữ liệu mới nhất.**

##### Lưu ý

Consistency trong CAP **không giống** Consistency trong ACID.

| ACID                                                           | CAP                                                                        |
| -------------------------------------------------------------- | -------------------------------------------------------------------------- |
| Dữ liệu luôn đúng theo các ràng buộc (Unique, Foreign Key,...) | Mọi server đều nhìn thấy cùng một phiên bản dữ liệu tại cùng một thời điểm |

Hay nói cách khác:

- **ACID** quan tâm dữ liệu **có hợp lệ không**.
- **CAP** quan tâm các **server có cùng nhìn thấy một dữ liệu hay không**.

##### <mark>A - Availability (Tính sẵn sàng)</mark>

Availability nghĩa là:

> **Mọi request gửi đến server đều phải nhận được phản hồi.**

Điều quan trọng là:

**Server phải phản hồi.**

Nó **không bắt buộc** dữ liệu phải mới nhất.

Ví dụ:

Server B vẫn chưa nhận được dữ liệu mới từ Server A.

Người dùng vẫn nhận được kết quả.

Server A : 4 sản phẩm

Server B : 3 sản phẩm

Dữ liệu sai một chút nhưng hệ thống vẫn hoạt động.

Đó chính là Availability.

---

##### <mark>P - Partition Tolerance (Khả năng chịu lỗi mạng)</mark>

Đây là phần nhiều người mới học khó hình dung nhất.

Hãy tưởng tượng ba server đang trao đổi dữ liệu với nhau.

![](C:\Users\hquan\AppData\Roaming\marktext\images\2026-07-12-00-21-13-image.png)

Bỗng nhiên đường truyền giữa A và B bị đứt.

![](C:\Users\hquan\AppData\Roaming\marktext\images\2026-07-12-00-21-30-image.png)

Lúc này:

- Server A vẫn chạy.
- Server B vẫn chạy.
- Nhưng hai server **không thể nói chuyện với nhau**.

Đây gọi là **Network Partition**.

Trong thực tế điều này xảy ra rất nhiều:

- Mất mạng.
- Switch hỏng.
- Router lỗi.
- Đứt cáp quang.
- Firewall chặn kết nối.
- Độ trễ mạng quá lớn.
- Một Data Center bị cô lập.

Một hệ thống tốt phải **tiếp tục hoạt động ngay cả khi xảy ra những sự cố này**.

Đó chính là **Partition Tolerance**.

---

##### Vì sao CAP luôn nói đến Partition?

Đây là điểm quan trọng nhất của CAP.

Nhiều người nghĩ rằng:

> **Có thể chọn CA, CP hoặc AP.**

Điều này chỉ đúng trên lý thuyết.

Trong thực tế:

> **Partition là điều chắc chắn sẽ xảy ra trong mọi hệ thống phân tán.**

Bạn không thể nói:

> "Hệ thống của tôi sẽ không bao giờ mất mạng."

Điều đó gần như là không thể.

Vì vậy, trong Distributed System:

![](C:\Users\hquan\AppData\Roaming\marktext\images\2026-07-12-00-24-40-image.png)

Không tồn tại hệ thống phân tán thực tế nào vừa đảm bảo **Consistency**, vừa đảm bảo **Availability** khi mạng giữa các node bị chia cắt.

---

##### Ví dụ thực tế

Giả sử có hai Data Center:

```
 Hà Nội  ─────────── TP.HCM    
 A                   B
```

Đột nhiên cáp quang bị đứt.

```
 Hà Nội      X      TP.HCM    
 A                B
```

Khách hàng tại Hà Nội vừa đặt hàng.

Lúc này có hai cách xử lý.

##### Cách 1: Ưu tiên Consistency (CP)

Server Hà Nội không thể đồng bộ dữ liệu sang TP.HCM.

Vì vậy nó quyết định:

```
❌ Không cho đặt hàng.
```

Đợi khi mạng ổn định rồi mới xử lý.

Ưu điểm:

- Dữ liệu luôn chính xác.

Nhược điểm:

- Người dùng không sử dụng được hệ thống.

---

##### Cách 2: Ưu tiên Availability (AP)

Server Hà Nội vẫn cho phép đặt hàng.

```
✔ Đặt hàng thành công
```

TP.HCM sẽ nhận được dữ liệu sau.

Ưu điểm:

- Hệ thống luôn hoạt động.

Nhược điểm:

- Một thời gian ngắn, dữ liệu giữa hai nơi sẽ khác nhau.

---

##### Tóm tắt dễ nhớ

| Thành phần              | Ý nghĩa                                                                |
| ----------------------- | ---------------------------------------------------------------------- |
| **Consistency**         | Đọc ở bất kỳ server nào cũng thấy cùng một dữ liệu mới nhất.           |
| **Availability**        | Server luôn phản hồi request, dù dữ liệu có thể chưa mới nhất.         |
| **Partition Tolerance** | Hệ thống vẫn tiếp tục hoạt động khi các server bị mất kết nối với nhau |

# 

## 2. PACELC — Mở rộng CAP cho đúng thực tế

CAP Theorem chỉ giải thích hệ thống sẽ hoạt động như thế nào **khi xảy ra sự cố chia mạng (Network Partition)**.

Nhưng trên thực tế, **phần lớn thời gian hệ thống vẫn hoạt động bình thường** và không hề có Partition. Vậy trong trường hợp này, chúng ta phải đánh đổi điều gì?

Năm 2012, **Daniel Abadi** đã đưa ra mô hình **PACELC** để trả lời câu hỏi đó:

> **Nếu có Partition (P):** phải chọn giữa **Availability (A)** hoặc **Consistency (C)**.
> 
> **Nếu không có Partition (Else):** phải chọn giữa **Latency (L)** hoặc **Consistency (C)**.

Nói đơn giản:

- Khi hệ thống gặp sự cố mạng, bạn phải quyết định **ưu tiên dữ liệu chính xác hay hệ thống luôn sẵn sàng phục vụ**.
- Khi hệ thống hoạt động bình thường, bạn lại phải quyết định **ưu tiên tốc độ phản hồi hay dữ liệu luôn đồng nhất**.

Để người nghe dễ nhớ, bạn có thể chốt bằng một câu duy nhất:

> **CAP trả lời: "Khi hệ thống gặp sự cố thì sao?"**  
> **PACELC trả lời: "Ngay cả khi không có sự cố, bạn vẫn phải đánh đổi giữa tốc độ và tính nhất quán của dữ liệu."**

---

## 3. BASE — Triết lý của hệ AP

Nếu **ACID** đặt mục tiêu dữ liệu luôn chính xác, thì **BASE** chấp nhận dữ liệu có thể chưa đồng bộ ngay lập tức để đổi lấy việc hệ thống luôn hoạt động.

BASE gồm 3 nguyên tắc:

#### B - Basically Available (Luôn sẵn sàng phục vụ)

Hệ thống sẽ **luôn cố gắng trả về phản hồi** cho người dùng, ngay cả khi dữ liệu chưa phải là mới nhất.

Ví dụ: Bạn vừa đổi ảnh đại diện Facebook nhưng mở ở một thiết bị khác vẫn thấy ảnh cũ trong vài giây. Dù vậy, Facebook vẫn hoạt động bình thường và không báo lỗi.

---

#### S - Soft State (Trạng thái có thể thay đổi theo thời gian)

Trong hệ phân tán, dữ liệu giữa các máy chủ không phải lúc nào cũng giống nhau.

Ngay cả khi không có ai cập nhật thêm dữ liệu, các máy chủ vẫn tiếp tục đồng bộ với nhau. Vì vậy, trạng thái của hệ thống có thể tự thay đổi theo thời gian.

---

#### E - Eventually Consistent (Cuối cùng sẽ đồng nhất)

Sau khi việc đồng bộ hoàn tất và không còn dữ liệu mới được ghi vào, tất cả các máy chủ sẽ dần cập nhật và **cuối cùng đều chứa cùng một dữ liệu**.

Ví dụ: Bạn đăng một bài viết mới. Ban đầu có người đã nhìn thấy bài viết, có người thì chưa. Nhưng chỉ sau vài giây hoặc vài phút, tất cả người dùng đều sẽ thấy cùng một nội dung.

#### Tóm tắt

BASE không yêu cầu dữ liệu phải đồng nhất ngay lập tức. Thay vào đó, hệ thống ưu tiên **luôn hoạt động**, và chấp nhận việc dữ liệu sẽ được đồng bộ dần theo thời gian.

##### Để người nghe podcast dễ nhớ, bạn có thể chốt bằng một câu:

> **ACID ưu tiên dữ liệu luôn đúng ngay lập tức. BASE ưu tiên hệ thống luôn hoạt động, còn dữ liệu sẽ đúng sau.**

---

## 4. Eventual Consistency — Đào sâu

Đây là một trong những khái niệm quan trọng nhất khi học **Microservice**. Nếu hiểu được Eventual Consistency, bạn sẽ dễ tiếp cận các mô hình như **Saga**, **Outbox Pattern** hay **Event-Driven Architecture**.

#### 4.1. Eventual Consistency là gì?

Hiểu đơn giản, dữ liệu **không cần giống nhau ngay lập tức** trên tất cả các máy chủ.

Thay vào đó, sau một khoảng thời gian đồng bộ, mọi bản sao của dữ liệu sẽ **giống hệt nhau**.

Ví dụ:

Bạn đăng một bài viết lên Facebook.

- Máy chủ A đã nhận được bài viết.
- Máy chủ B vẫn chưa kịp cập nhật.
- Một lát sau, tất cả máy chủ đều có bài viết đó.

Đó chính là **Eventual Consistency**.

Điều cần lưu ý là từ **"một khoảng thời gian"** không có con số cố định.

Có thể chỉ vài chục mili giây, vài giây hoặc lâu hơn, tùy vào thiết kế và tải của hệ thống.

---

#### 4.2. Các mức độ của Eventual Consistency

Không phải hệ thống Eventual Consistency nào cũng hoạt động giống nhau. Có nhiều mức độ khác nhau để mang lại trải nghiệm tốt hơn.

##### Read Your Writes

Sau khi **bạn vừa ghi dữ liệu**, chính bạn sẽ nhìn thấy dữ liệu mới ngay lập tức.

Người khác có thể vẫn chưa thấy.

Ví dụ:

Bạn vừa đăng một bình luận.

Bạn phải nhìn thấy bình luận của mình ngay, nếu không sẽ tưởng việc gửi bình luận bị lỗi.

---

##### Monotonic Reads

Nếu bạn đã nhìn thấy phiên bản mới của dữ liệu thì những lần đọc sau sẽ **không bao giờ quay lại phiên bản cũ**.

Ví dụ:

Bạn refresh trang.

Lần đầu thấy có 10 bình luận.

Lần sau lại chỉ còn 9 bình luận.

Điều này khiến người dùng có cảm giác dữ liệu đang "quay ngược thời gian". Monotonic Reads giúp tránh tình huống đó.

---

##### Monotonic Writes

Các thao tác cập nhật của cùng một người dùng phải được xử lý đúng thứ tự.

Ví dụ:

Bạn đổi tên tài khoản.

Sau đó mới đổi ảnh đại diện.

Hệ thống phải xử lý theo đúng thứ tự này, không được đổi ảnh trước rồi mới đổi tên.

---

##### Causal Consistency

Nếu một hành động phụ thuộc vào hành động khác thì mọi người đều phải nhìn thấy đúng thứ tự.

Ví dụ:

Có một bình luận gốc.

Sau đó mới có bình luận trả lời.

Không ai được nhìn thấy bình luận trả lời trước khi thấy bình luận gốc.

---

##### Strong Eventual Consistency

Đây là phiên bản mạnh hơn của Eventual Consistency.

Dù các máy chủ nhận dữ liệu theo thứ tự khác nhau, cuối cùng chúng vẫn tự đồng bộ để có cùng một kết quả mà không cần phối hợp với nhau.

Mô hình này thường được triển khai bằng **CRDT (Conflict-free Replicated Data Type)**.

---

#### 4.3. Ví dụ thực tế

##### Giỏ hàng thương mại điện tử

Bạn thêm sản phẩm vào giỏ hàng trên điện thoại.

Ngay sau đó mở website thì chưa thấy sản phẩm xuất hiện.

Khoảng 1–2 giây sau, tải lại trang thì sản phẩm đã có.

Đây là Eventual Consistency và hoàn toàn chấp nhận được.

---

##### Chuyển tiền ngân hàng

Bạn chuyển 10 triệu đồng.

Nếu 5 giây sau người nhận vẫn chưa thấy tiền thì đây là vấn đề nghiêm trọng.

Những nghiệp vụ tài chính thường không chấp nhận Eventual Consistency mà cần dữ liệu chính xác ngay lập tức hoặc phải sử dụng các cơ chế như **Strong Consistency** hay **Saga** để đảm bảo tính đúng đắn.

---

##### Trạng thái đơn hàng

Bạn vừa đặt hàng.

Ngay sau đó mở trang chi tiết đơn hàng nhưng hệ thống vẫn hiển thị **"Đang xử lý"**.

Vài giây sau trạng thái chuyển sang **"Đã tạo thành công"**.

Đây cũng là Eventual Consistency và là cách hoạt động rất phổ biến trong các hệ thống Microservice.

---

## 5. Strong vs Eventual Consistency — Khi nào dùng cái nào?

Giả sử bạn có **3 server** cùng lưu thông tin của một tài khoản ngân hàng.

![](C:\Users\hquan\AppData\Roaming\marktext\images\2026-07-11-23-59-03-image.png)

Khi người dùng cập nhật dữ liệu, câu hỏi là:

> **Bao giờ các server còn lại sẽ nhìn thấy dữ liệu mới?**

Đó chính là điểm khác nhau giữa **Strong Consistency** và **Eventual Consistency**.

##### <mark> Strong Consistency (Mạnh)</mark>

Hình dung

Bạn vừa chuyển **10 triệu đồng**.

![](C:\Users\hquan\AppData\Roaming\marktext\images\2026-07-12-00-00-25-image.png)

👉 Tất cả server đều phải cập nhật xong rồi mới cho phép đọc.

Nói cách khác:

> **Dù bạn đọc ở server nào cũng luôn nhận được dữ liệu mới nhất.**

---

**Khi nào bắt buộc dùng?**

Những nơi **không được phép sai dù chỉ một lần**.

Ví dụ:

- 💳 Chuyển tiền ngân hàng
- 🏦 Thanh toán
- 📦 Trừ tồn kho (tránh bán vượt số lượng)
- 🔑 Đăng nhập, xác thực người dùng
- 👑 Leader Election trong Cluster

Ví dụ:

Kho chỉ còn **1 chiếc iPhone**.

Có hai khách đặt cùng lúc.

Nếu cả hai đều đọc thấy còn hàng thì hệ thống sẽ bán **2 chiếc**, trong khi kho chỉ có **1 chiếc**.

=> Không thể chấp nhận.

---

**Để đạt được Strong Consistency cần gì?**

Các server phải **chờ nhau đồng bộ xong** rồi mới trả kết quả.

![](C:\Users\hquan\AppData\Roaming\marktext\images\2026-07-12-00-01-33-image.png)

Một số kỹ thuật:

- Synchronous Replication
- Paxos
- Raft
- 2PC (Two Phase Commit)

---

**Đánh đổi**

Vì phải chờ tất cả server nên:

- ❌ Chậm hơn
- ❌ Nếu một server bị lỗi hoặc mất mạng có thể không ghi được dữ liệu
- ✅ Nhưng dữ liệu luôn chính xác

##### <mark>Eventual Consistency (Nhất quán cuối cùng)</mark>

**Hình dung**

Bạn vừa đăng một bài viết lên Facebook.

![](C:\Users\hquan\AppData\Roaming\marktext\images\2026-07-12-00-04-34-image.png)

👉 Trong vài giây đầu, dữ liệu giữa các server có thể khác nhau.

Sau một thời gian ngắn, tất cả sẽ giống nhau.

Đó chính là **Eventual Consistency**.

---

Khi nào nên dùng?

Những nơi **không cần chính xác ngay lập tức**.

Ví dụ:

- 👍 Like Facebook
- ❤️ Tim TikTok
- 💬 Bình luận
- 📰 News Feed
- 👀 View Count
- 🔍 Search Index
- 🤖 Recommendation
- 📈 Analytics

Một số kỹ thuật:

- Async Replication
- Gossip Protocol
- CRDT

---

**Đánh đổi**

Ưu điểm

- ✅ Phản hồi rất nhanh
- ✅ Dễ mở rộng
- ✅ Hệ thống luôn hoạt động

Nhược điểm

- ❌ Một thời gian ngắn dữ liệu có thể chưa giống nhau
- ❌ Phải xử lý thêm các trường hợp đọc dữ liệu cũ (stale data)

## 6. Áp dụng vào một hệ thống Microservice thực tế

Giả sử chúng ta có quy trình xử lý đơn hàng như sau:

> **Order → Inventory → Payment → Shipping**

Khi khách hàng đặt hàng, hệ thống sẽ:

- Tạo đơn hàng.

- Kiểm tra và trừ tồn kho.

- Thanh toán.

- Giao hàng.

Lúc này sẽ có ba câu hỏi rất quan trọng.

---

#### Câu hỏi 1: Nên dùng Strong Consistency hay Eventual Consistency?

Có hai cách để xây dựng hệ thống.

##### Cách 1: Strong Consistency

Tất cả các service phải hoàn thành hoặc cùng thất bại.

Điều này thường phải dùng **Distributed Transaction (2PC/XA)**.

Ưu điểm là dữ liệu luôn đồng nhất.

Nhược điểm là:

- Hệ thống chậm hơn.

- Khó mở rộng.

- Chỉ cần một service gặp sự cố là cả giao dịch phải chờ.

---

##### Cách 2: Eventual Consistency

Mỗi service có cơ sở dữ liệu riêng.

Sau khi hoàn thành công việc của mình, service sẽ gửi **Event** để các service khác tiếp tục xử lý.

Ưu điểm:

- Nhanh hơn.

- Dễ mở rộng.

- Ít phụ thuộc lẫn nhau.

Đổi lại, dữ liệu có thể chưa đồng bộ ngay lập tức.

👉 Vì vậy, **hầu hết các hệ thống Microservice hiện nay đều chọn Eventual Consistency thay vì Strong Consistency.**

---

#### Câu hỏi 2: Nếu dữ liệu chưa đồng bộ ngay thì ai đảm bảo cuối cùng mọi thứ đều đúng?

Đó chính là lý do các hệ thống Microservice sử dụng nhiều Design Pattern.

| Vấn đề                                            | Giải pháp                            |
| ------------------------------------------------- | ------------------------------------ |
| Lưu dữ liệu và gửi Event phải thành công cùng lúc | **Outbox Pattern**                   |
| Quản lý toàn bộ quy trình nhiều bước              | **Saga Pattern**                     |
| Event bị xử lý nhiều lần                          | **Idempotency**                      |
| Một bước thất bại thì hoàn tác các bước trước     | **Compensating Transaction**         |
| Service tạm thời bị lỗi                           | **Kafka** lưu Event để xử lý lại sau |

Có thể hiểu đơn giản:

Những Pattern này giúp hệ thống **tự sửa lỗi và tự đồng bộ**, thay vì bắt tất cả service phải hoàn thành cùng một lúc.

---

#### Câu hỏi 3: Người dùng sẽ thấy gì?

Đây là điều một Senior Developer luôn phải nghĩ đến trước khi viết code.

Ví dụ:

##### Tạo đơn hàng

Người dùng vừa nhấn nút **Đặt hàng**.

Nếu bấm F5 ngay lập tức có thể chưa thấy đơn hàng xuất hiện.

Giải pháp là giao diện hiển thị ngay đơn hàng mới với trạng thái đang xử lý, thay vì bắt người dùng chờ.

---

##### Thanh toán

Sau khi bấm thanh toán, không nên hiển thị ngay **"Thanh toán thành công"**.

Thay vào đó nên hiển thị:

> **Đang xử lý...**

Khi Payment Service xác nhận xong mới đổi sang **Thành công**.

---

##### Tồn kho

Đơn hàng đã tạo nhưng Inventory Service chưa kịp trừ số lượng.

Nếu nhiều người mua cùng lúc có thể bán vượt số lượng thực tế.

Để tránh điều này, các hệ thống thường dùng **Reservation Pattern** để giữ hàng tạm thời.

---

##### Email xác nhận

Không nên gửi email ngay sau khi tạo Order.

Chỉ nên gửi khi toàn bộ quy trình như thanh toán và xử lý đơn hàng đã hoàn tất.

---

## 7. Những sai lầm người mới thường gặp

#### Sai lầm 1: Nhầm Consistency trong ACID và CAP

Hai khái niệm này hoàn toàn khác nhau.

- **ACID** nói về tính đúng đắn của dữ liệu trong một Database.

- **CAP** nói về việc dữ liệu có đồng bộ giữa nhiều máy chủ hay không.

Đây là lỗi rất hay gặp trong các buổi phỏng vấn.

---

#### Sai lầm 2: Nghĩ rằng hệ thống có thể chọn CA

Trong hệ thống phân tán, **Partition là điều chắc chắn sẽ xảy ra**.

Vì vậy không thể nói:

> "Hệ thống của tôi chọn CA."

Nếu ai nói như vậy thì gần như chưa hiểu CAP Theorem.

---

#### Sai lầm 3: Nghĩ rằng Eventual Consistency là đủ

Eventual Consistency không có nghĩa là muốn đồng bộ lúc nào cũng được.

Bạn vẫn phải thiết kế giao diện để người dùng hiểu điều gì đang xảy ra.

Ví dụ:

- Hiển thị trạng thái **Đang xử lý**.

- Có nút tải lại.

- Có thông báo khi dữ liệu đã cập nhật.

---

#### Sai lầm 4: Không quan tâm dữ liệu đồng bộ mất bao lâu

Eventual Consistency không có nghĩa là chờ vài phút cũng được.

Trong hệ thống thật, đội vận hành luôn theo dõi thời gian đồng bộ.

Ví dụ:

- 95% dữ liệu đồng bộ trong dưới 100ms.

- 99% dữ liệu đồng bộ trong dưới 500ms.

Đó là chỉ số rất quan trọng để đánh giá chất lượng hệ thống.

---

#### Sai lầm 5: Dùng 2PC cho mọi Microservice

Nhiều người nghĩ chỉ cần dùng 2PC là sẽ có ACID giữa nhiều service.

Thực tế, 2PC có nhiều nhược điểm:

- Hệ thống chậm hơn.

- Khó mở rộng.

- Nếu Coordinator gặp sự cố, toàn bộ giao dịch có thể bị chặn.

Vì vậy, phần lớn các hệ thống hiện đại đã chuyển sang **Saga Pattern** thay vì sử dụng 2PC.

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
