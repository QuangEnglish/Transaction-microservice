# Saga Pattern: Orchestration vs Choreography

> Bài 4 trong lộ trình Transaction Microservice nâng cao.
> Mục tiêu: Hiểu sâu Saga Pattern — cách industry hiện đại quản lý transaction xuyên nhiều microservice. Phân biệt 2 biến thể Orchestration và Choreography, biết khi nào dùng cái nào, và cách áp dụng vào flow `Order → Inventory → Payment → Shipping`.

---

## 0. Nhắc lại context

- **Bài 1-2:** ACID không thể trải dài qua nhiều DB. Microservices buộc phải chọn Eventual Consistency.
- **Bài 3:** 2PC/XA tồn tại nhưng blocking, không scale, không hợp Kafka/REST → industry bỏ.
- **Bài 4 (bài này):** **Saga** là gì, hoạt động ra sao, làm sao thay thế được distributed transaction.

---

### 1. Saga là gì?

## 1.1. Định nghĩa

> **Saga** là một cách xử lý giao dịch trong **microservices**, trong đó **mỗi service chỉ thực hiện transaction của chính mình**. Nếu một bước gặp lỗi, hệ thống sẽ **thực hiện các transaction bù (Compensating Transaction)** để đưa toàn bộ quy trình về trạng thái hợp lý.

Saga được giới thiệu lần đầu trong bài báo **"Sagas"** của Hector Garcia-Molina và Kenneth Salem (1987). Ban đầu nó được dùng cho các transaction kéo dài trong một cơ sở dữ liệu, nhưng ngày nay lại trở thành giải pháp phổ biến cho kiến trúc microservices.

---

## 1.2. Ý tưởng cốt lõi

Ở bài trước chúng ta đã thấy **2PC** cố gắng làm cho tất cả service **commit hoặc rollback cùng lúc**. Điều này giúp dữ liệu luôn nhất quán nhưng phải giữ lock lâu, hiệu năng giảm và khó mở rộng.

**Saga đi theo hướng hoàn toàn khác.**

Thay vì đợi tất cả service hoàn thành rồi mới commit:

1. Mỗi service xử lý **local transaction** của mình.
2. Nếu thành công thì **commit ngay**, không chờ service khác.
3. Sau khi commit, service gửi **Event** hoặc **Command** để kích hoạt bước tiếp theo.
4. Nếu một bước phía sau thất bại, hệ thống sẽ chạy **Compensating Transaction** để bù lại những gì các bước trước đã làm.

Có thể hình dung như sau:

```
Order Service      │      ▼Commit Order      │ Publish Event      │      ▼Inventory Service      │ Commit Inventory      │ Publish Event      │      ▼Payment Service      │      ✖ Fail      │      ▼Compensating Transaction      │Khôi phục Inventory      │Khôi phục Order
```

Điểm khác biệt lớn nhất là:

- **2PC:** Chưa ai commit cho đến khi tất cả đều sẵn sàng.
- **Saga:** Mỗi service commit ngay khi hoàn thành phần việc của mình.

---

## 1.3. Saga đánh đổi điều gì?

Không có mô hình nào hoàn hảo, Saga cũng vậy.

Bạn sẽ **đánh đổi một số tính chất của ACID** để đổi lấy khả năng mở rộng.

### Mất

- **Isolation**: Trong lúc Saga đang chạy, các service khác có thể nhìn thấy dữ liệu ở trạng thái "đang xử lý", chưa hoàn chỉnh.
- **Atomicity tuyệt đối**: Không còn kiểu "thành công tất cả hoặc rollback tất cả ngay lập tức".

### Được

- **Availability cao hơn**: Mỗi service hoạt động độc lập, không phải chờ nhau.
- **Scalability tốt hơn**: Không giữ lock lâu như 2PC.
- **Loose Coupling**: Các service giao tiếp qua Event hoặc Command nên ít phụ thuộc lẫn nhau.

Đây chính là lý do phần lớn hệ thống microservices hiện đại ưu tiên Saga thay vì Distributed Transaction.

---

# 1.4. Compensating Transaction – Trái tim của Saga

Đây là khái niệm quan trọng nhất khi học Saga.

> **Compensating Transaction** là **một transaction mới** được tạo ra để **bù lại tác động của một transaction trước đó** khi quy trình xảy ra lỗi.

Điều quan trọng cần nhớ là:

> **Compensation KHÔNG phải Rollback.**

Đây là hai khái niệm hoàn toàn khác nhau.

### Rollback (ACID)

Rollback xảy ra khi transaction **chưa commit**.

Khi rollback, mọi thay đổi đều bị xóa như chưa từng tồn tại.

```
Update Database        │Rollback        │Database trở về trạng thái ban đầu
```

Giống như bạn gõ một đoạn văn trong Word nhưng chưa lưu, sau đó nhấn **Undo**. Đoạn văn biến mất hoàn toàn.

---

### Compensation (Saga)

Trong Saga, transaction trước **đã commit thành công**.

Lúc này không thể rollback được nữa.

Giải pháp là tạo **một transaction mới** để bù lại tác động trước đó.

Ví dụ:

```
T1: Trừ 100.000đ từ tài khoản A   ✔ CommitT2: Cộng 100.000đ vào tài khoản B ✖ FailC1: Cộng lại 100.000đ vào A        ✔ Commit
```

Kết quả cuối cùng:

```
A: 1.000.000đB: Không thay đổi
```

Trong lịch sử giao dịch vẫn còn đầy đủ:

```
✔ Trừ tiền✖ Chuyển tiền thất bại✔ Cộng tiền hoàn lại
```

Không có giao dịch nào bị "xóa khỏi lịch sử". Hệ thống chỉ ghi thêm một giao dịch để cân bằng lại dữ liệu.

---

# 1.5. Một Compensating Transaction tốt cần có gì?

Để Saga hoạt động ổn định, transaction bù thường cần các đặc điểm sau:

### Idempotent

Nếu vì lý do nào đó transaction bù bị chạy nhiều lần thì kết quả vẫn phải giống như chỉ chạy một lần.

Ví dụ:

```
Refund Order #100
```

Dù message bị gửi lại 3 lần thì khách hàng cũng chỉ được hoàn tiền một lần.

---

### Semantically Correct

Compensation phải phản ánh đúng ý nghĩa nghiệp vụ.

Ví dụ:

```
Đặt hàng thất bại→ Trả lại tồn kho→ Hủy đơn hàng→ Hoàn tiền
```

Đó mới là cách "undo" đúng theo góc nhìn của business.

---

### Commutative (nếu có thể)

Nếu các transaction bù có thể thực hiện theo nhiều thứ tự khác nhau mà kết quả vẫn đúng thì hệ thống sẽ dễ mở rộng và xử lý lỗi hơn.

Đây là một đặc tính tốt nên có, nhưng không phải lúc nào cũng đạt được.

---

# 1.6. Không phải thứ gì cũng "undo" được

Một hiểu lầm phổ biến là nghĩ rằng Compensation có thể đưa mọi thứ về trạng thái ban đầu.

Thực tế thì **không phải lúc nào cũng làm được**.

Ví dụ:

### Gửi Email

```
Đã gửi email cho khách
```

Không có API nào có thể thu hồi email đã gửi.

Giải pháp thường là:

```
Gửi thêm email xin lỗi
```

Đó chính là Compensation.

---

### Thanh toán

```
Đã trừ tiền khách hàng
```

Không thể "xóa" giao dịch khỏi ngân hàng.

Giải pháp là:

```
Refund
```

Khách nhận lại tiền bằng **một giao dịch mới**.

---

### Giao hàng

```
Đơn hàng đã giao
```

Không thể gọi hàm:

```
undoShipping();
```

Giải pháp thực tế là:

- Yêu cầu khách trả hàng.
- Tạo phiếu hoàn hàng.
- Hoàn tiền sau khi nhận lại sản phẩm.

Đó đều là các **Compensating Transaction** ở mức nghiệp vụ.

---

## 2. Anatomy của một Saga

## 2.1. Một Saga gồm những gì?

Một Saga thường có **3 thành phần chính**:

- **Saga Definition:** Quy định các bước cần thực hiện (T1 → T2 → T3...) và các bước bù tương ứng (C1, C2...).
- **Saga Coordinator:** Thành phần điều phối, quyết định bước nào chạy tiếp hoặc khi nào cần bù.
- **Saga Log:** Lưu trạng thái của từng bước để có thể khôi phục nếu hệ thống bị crash.

> Hiểu đơn giản:
> 
> - **Definition** = Kịch bản.
> - **Coordinator** = Người điều khiển.
> - **Log** = Nhật ký theo dõi.

---

## 2.2. Có 3 cách xử lý khi Saga gặp lỗi

### 1. Backward Recovery (phổ biến nhất)

Nếu một bước thất bại thì **quay ngược lại và chạy các transaction bù**.

Ví dụ:

```
Đặt hàng ✔Giữ hàng ✔Thanh toán ✖→ Trả lại hàng→ Hủy đơn
```

Đây là cách hầu hết hệ thống thương mại điện tử sử dụng.

---

### 2. Forward Recovery

Không quay lại, mà **liên tục retry cho đến khi thành công**.

Ví dụ:

```
Kê khai thuế ✖→ Retry→ Retry→ Retry→ Thành công
```

Áp dụng khi **bắt buộc phải hoàn thành**, không thể hủy.

---

### 3. Mixed Recovery

Kết hợp cả hai cách trên.

- Có bước thì rollback bằng compensation.
- Có bước thì retry đến khi thành công.

Đây là mô hình phổ biến nhất trong các hệ thống thực tế.

## 2.3. Các trạng thái của một Saga

PENDING
    │
    ▼
IN_PROGRESS
    │
 ┌──┴───────────┐
 ▼              ▼
COMPLETED    FAILED
                 │
                 ▼
          COMPENSATING
                 │
        ┌────────┴────────┐
        ▼                 ▼
  COMPENSATED   COMPENSATION_FAILED
                      │
                Cần người xử lý



## 3. Hai cách triển khai Saga: Choreography và Orchestration

Khi thiết kế Saga, bạn sẽ phải chọn **một trong hai cách điều phối**:

- **Choreography (Biên đạo: [ˌkɔːr.iˈɑː.ɡrə.fi])** → Không có người điều khiển trung tâm.
- **Orchestration (Hòa âm: [ˌɔrkəˈstreɪʃən])** → Có một service đứng ra điều phối toàn bộ quy trình.

---

## 3.1. Choreography – Mỗi service tự biết việc của mình

> **Không có Coordinator.** Mỗi service chỉ cần **lắng nghe Event** và khi hoàn thành công việc thì **phát Event mới** để service tiếp theo xử lý.

Ví dụ luồng đặt hàng:

Order
  │
  ▼
OrderCreated Event
  │
  ▼
Inventory giữ hàng
  │
  ▼
StockReserved Event
  │
  ▼
Payment thanh toán
  │
  ▼
PaymentCompleted Event
  │
  ▼
Shipping giao hàng

**Nếu thanh toán thất bại:**

PaymentFailed Event
        │
        ▼
Inventory trả lại hàng
        │
        ▼
Order hủy đơn

Mỗi service chỉ quan tâm **Event** của mình, không cần biết các service khác hoạt động như thế nào.

### Ưu điểm

- Service ít phụ thuộc nhau (Loose Coupling).
- Dễ mở rộng, chỉ cần thêm service lắng nghe Event.
- Không có "điểm chết" trung tâm.
- Phù hợp với các quy trình đơn giản.

### Nhược điểm

- Khó theo dõi toàn bộ luồng xử lý.
- Debug khó vì logic nằm rải rác ở nhiều service.
- Compensation phức tạp.
- Dễ tạo vòng lặp Event nếu thiết kế không cẩn thận.

### Khi nào nên dùng?

- Flow ngắn (khoảng 3–4 bước).
- Logic đơn giản.
- Hệ thống sử dụng Event-Driven Architecture.

---

## 3.2. Orchestration – Có một "nhạc trưởng"

> Có một **Saga Orchestrator** đứng giữa, chịu trách nhiệm điều phối toàn bộ quy trình.

Các service không tự quyết định bước tiếp theo mà **chỉ làm theo lệnh của Orchestrator**.

Ví dụ:

Client
   │
   ▼
Saga Orchestrator
   │
   ├──► Order Service
   ├──► Inventory Service
   ├──► Payment Service
   └──► Shipping Service

**Luồng xử lý:**

Orchestrator
      │
      ▼
Tạo Order
      │
      ▼
Giữ hàng
      │
      ▼
Thanh toán
      │
      ▼
Giao hàng
      │
      ▼
Hoàn thành

**Nếu Payment thất bại:**

Payment Fail
      │
      ▼
Orchestrator
      │
      ├──► Trả lại hàng
      └──► Hủy đơn

**Luồng xử lý:**

Orchestrator
      │
      ▼
Tạo Order
      │
      ▼
Giữ hàng
      │
      ▼
Thanh toán
      │
      ▼
Giao hàng
      │
      ▼
Hoàn thành

**Nếu Payment thất bại:**

Payment Fail
      │
      ▼
Orchestrator
      │
      ├──► Trả lại hàng
      └──► Hủy đơn

Orchestrator luôn biết quy trình đang ở bước nào và cần làm gì tiếp theo.

### Cách giao tiếp

Orchestrator thường dùng:

- **Command:** Ra lệnh cho service thực hiện công việc.
- **Reply:** Service phản hồi thành công hoặc thất bại.

Ví dụ:

Orchestrator
 │
ChargePaymentCommand
 │
 ▼
Payment Service
 │
PaymentCompletedReply

### Ưu điểm

- Toàn bộ luồng xử lý tập trung ở một nơi.

**"Logic tập trung ở 1 nơi" không có nghĩa là các service không có business logic.**

Ý của nó là:

- **Business logic của từng service** vẫn nằm trong service đó.
- **Business flow (workflow)** của toàn bộ quy trình thì nằm ở Orchestrator.

- Dễ đọc, dễ debug.
- Compensation rõ ràng.
- Phù hợp với quy trình nhiều bước và nhiều điều kiện.

### Nhược điểm

- Orchestrator có thể trở thành "điểm chết" nếu không triển khai HA.
- Dễ trở thành **God Service** nếu nhồi quá nhiều business logic.
- Orchestrator phải biết về tất cả service.

### Khi nào nên dùng?

- Flow dài (trên 4 bước).
- Có nhiều điều kiện, retry hoặc timeout.
- Compensation phức tạp.
- Cần theo dõi, audit và monitoring chi tiết.

### <mark> Một câu rất hay để ghi nhớ:</mark>

> **Orchestrator quyết định "ai làm tiếp theo", còn mỗi Service quyết định "làm việc đó như thế nào".**

## 3.3. So sánh nhanh

| Choreography              | Orchestration                    |
| ------------------------- | -------------------------------- |
| Không có Coordinator      | Có Orchestrator                  |
| Giao tiếp bằng **Event**  | Giao tiếp bằng **Command/Reply** |
| Logic nằm ở nhiều service | Logic tập trung một nơi          |
| Khó debug                 | Dễ debug                         |
| Dễ mở rộng                | Dễ quản lý quy trình             |
| Phù hợp flow đơn giản     | Phù hợp flow phức tạp            |

### Ghi nhớ nhanh

- **Choreography** = **Mỗi service tự nghe Event và tự làm việc.**
- **Orchestration** = **Có một "nhạc trưởng" điều khiển toàn bộ quy trình.**

> **Quy tắc kinh nghiệm:**
> 
> - **Flow đơn giản (≤ 4 bước)** → Ưu tiên **Choreography**.
> - **Flow phức tạp (> 4 bước, nhiều nhánh, nhiều bước bù)** → Ưu tiên **Orchestration**.

## 4. Saga thực hiện flow của bạn — Thiết kế chi tiết

Nếu mọi thứ diễn ra suôn sẻ thì Saga sẽ chạy lần lượt như sau:Chi tiết từng bước:

Khách đặt hàng
      │
      ▼
Order Service
(Tạo đơn hàng)
      │
      ▼
Inventory Service
(Giữ hàng)
      │
      ▼
Payment Service
(Thanh toán)
      │
      ▼
Shipping Service
(Tạo đơn giao hàng)
      │
      ▼
Order Service
(Cập nhật đơn hàng hoàn thành)

### Bước 1. Order Service

- Tạo đơn hàng.
- Trạng thái ban đầu là **PENDING**.
- Gửi Event **OrderCreated**.

↓

### Bước 2. Inventory Service

- Nhận Event từ Order.
- Giữ số lượng hàng cần bán.
- Gửi Event **StockReserved**.

↓

### Bước 3. Payment Service

- Nhận yêu cầu thanh toán.
- Gọi cổng thanh toán (VNPay, Stripe,...).
- Nếu thành công thì lưu giao dịch.
- Gửi Event **PaymentCompleted**.

↓

### Bước 4. Shipping Service

- Tạo đơn giao hàng.
- Gọi API của đơn vị vận chuyển.
- Gửi Event **ShipmentCreated**.

↓

### Bước 5. Order Service

- Nhận Event ShipmentCreated.
- Cập nhật trạng thái đơn hàng thành **CONFIRMED**.

#### Ghi nhớ nhanh

- **Happy Path**: Order → Inventory → Payment → Shipping → Hoàn thành.
- **Payment lỗi**: Trả hàng → Hủy đơn.
- **Mỗi service có Database riêng**, không truy cập DB của nhau.
- **Saga hoạt động như một State Machine**, mỗi Event sẽ làm quy trình chuyển sang trạng thái tiếp theo hoặc bắt đầu Compensation khi có lỗi.

## 5. Implementation trong Spring Boot

### 5.1. Choreography với Kafka

**OrderService:**

```java
@Service
public class OrderService {
    @Transactional
    public Order createOrder(OrderRequest req) {
        Order order = orderRepository.save(new Order(req, "PENDING"));
        // Outbox pattern - học bài 5
        outboxRepository.save(new OutboxEvent("OrderCreated", order.toJson()));
        return order;
    }
}

@Component
public class OrderEventListener {
    @KafkaListener(topics = "shipment-events")
    @Transactional
    public void onShipmentCreated(ShipmentCreatedEvent e) {
        orderRepository.updateStatus(e.getOrderId(), "CONFIRMED");
    }

    @KafkaListener(topics = "payment-events")
    @Transactional
    public void onPaymentFailed(PaymentFailedEvent e) {
        orderRepository.updateStatus(e.getOrderId(), "CANCELLED");
    }
}
```

**InventoryService:**

```java
@Component
public class InventoryEventListener {
    @KafkaListener(topics = "order-events")
    @Transactional
    public void onOrderCreated(OrderCreatedEvent e) {
        try {
            inventoryService.reserve(e.getProductId(), e.getQty());
            outboxRepository.save(new OutboxEvent("StockReserved", ...));
        } catch (InsufficientStockException ex) {
            outboxRepository.save(new OutboxEvent("StockReserveFailed", ...));
        }
    }

    @KafkaListener(topics = "payment-events")
    @Transactional
    public void onPaymentFailed(PaymentFailedEvent e) {
        inventoryService.release(e.getProductId(), e.getQty());
        outboxRepository.save(new OutboxEvent("StockReleased", ...));
    }
}
```

### 5.2. Orchestration với State Machine

Cách 1: Tự viết với Spring State Machine

```java
@Configuration
@EnableStateMachineFactory
public class OrderSagaStateMachine 
    extends EnumStateMachineConfigurerAdapter<OrderState, OrderEvent> {

    @Override
    public void configure(StateMachineTransitionConfigurer<OrderState, OrderEvent> t) 
        throws Exception {
        t.withExternal()
            .source(OrderState.NEW)
            .target(OrderState.STOCK_RESERVED)
            .event(OrderEvent.RESERVE_STOCK_SUCCESS)
         .and().withExternal()
            .source(OrderState.STOCK_RESERVED)
            .target(OrderState.PAYMENT_COMPLETED)
            .event(OrderEvent.PAYMENT_SUCCESS)
         .and().withExternal()
            .source(OrderState.STOCK_RESERVED)
            .target(OrderState.COMPENSATING_STOCK)
            .event(OrderEvent.PAYMENT_FAILED)
        // ...
        ;
    }
}
```

Cách 2: Framework chuyên dụng

- **Axon Framework**: hỗ trợ Saga tốt
- **Camunda / Zeebe**: BPMN-based, mạnh cho flow phức tạp
- **Eventuate Tram Sagas**: framework chuyên Saga (của Chris Richardson)
- **Temporal.io / Cadence**: workflow engine xịn nhất hiện tại, được Uber/Coinbase dùng



---

## 6. Khi nào KHÔNG dùng Saga

Saga rất mạnh nhưng không phải là nhất. **Không dùng** khi:

1. **Strong consistency là bắt buộc về mặt pháp lý/business**: ví dụ chuyển khoản liên ngân hàng → dùng XA (vâng, banking vẫn dùng 2PC).
2. **Flow đơn giản, 1 service** → chỉ cần `@Transactional` thường.
3. **Compensation không thể thực hiện được**: nếu một bước "không thể undo" và business không chấp nhận inconsistency → cần redesign.
4. **Team chưa có culture event-driven** → Saga sai cách còn tệ hơn không có Saga.
5. **Đang ở giai đoạn early startup**: monolith có lẽ tốt hơn (nhưng chuẩn bị tinh thần migrate sau).

---

## 7. Bẫy phổ biến

### Bẫy 1: Không design Compensation từ đầu

Bắt đầu code happy path, rồi mới nghĩ "à phải có compensation". Sai. Saga phải design **cả 2 chiều cùng lúc**.

### Bẫy 2: Compensation không idempotent

Network retry → compensation chạy 2 lần → trừ kho 2 lần → âm kho. → **Mọi compensation phải idempotent** (bài 6).

### Bẫy 3: Quên Isolation

Saga **không có Isolation**. Giai đoạn giữa flow, data có thể inconsistent. Ví dụ:

- Order đã tạo (PENDING), stock đã reserve, payment đang xử lý
- User mở app khác → thấy order "đang xử lý" nhưng stock đã trừ tạm thời
- Cần design business semantics: "Reserved" stock không hiển thị cho user khác

### Bẫy 4: God Orchestrator

Orchestrator chứa quá nhiều business logic, trở thành mini-monolith. Quy tắc: **Orchestrator chỉ điều phối, không xử lý business**. Business logic vẫn ở các service.

### Bẫy 5: Saga timeout

Saga có thể chạy lâu (phút, giờ — Long-running Saga). Cần:

- Timeout cho từng bước
- Timeout cho toàn Saga
- Cleanup mechanism (cron job)

### Bẫy 6: Không có Saga Log

Khi orchestrator crash, restart không biết đang ở bước nào → bí. **Saga Log** lưu trạng thái phải persisted, không phải in-memory.

### Bẫy 7: Compensation order sai

Phải compensate theo **đúng thứ tự ngược lại** với forward. Compensate "stock" trước "payment" có thể gây lỗi business.

---

## 8. Cầu nối tới bài tiếp theo

Bạn đã hiểu cách Saga hoạt động và cách viết Orchestrator cơ bản. Nhưng vẫn còn một vấn đề rất quan trọng trong thực tế.

> **Làm sao để việc lưu dữ liệu vào Database và gửi Event lên Kafka luôn đồng bộ với nhau?**

Ví dụ trong `OrderService`:

```
orderRepository.save(order);   // Lưu đơn hàng
kafkaTemplate.send(...);        // Gửi Event OrderCreated
```

Hai thao tác này là **2 hệ thống khác nhau**:

- Database
- Kafka

Vì vậy, chúng **không thể commit cùng lúc** như một transaction ACID.

---

##### Trường hợp 1: Lưu DB thành công, gửi Kafka thất bại

```
Lưu Order vào DB      ✔Gửi Event lên Kafka   ✖
```

Kết quả:

- Đơn hàng đã có trong Database.
- Nhưng không có Event `OrderCreated`.
- Inventory Service không biết có đơn hàng mới để xử lý.

➡️ Saga sẽ **dừng giữa chừng**.

---

##### Trường hợp 2: Gửi Kafka thành công, lưu DB thất bại

```
Lưu Order vào DB      ✖Gửi Event lên Kafka   ✔
```

Kết quả:

- Inventory Service nhận được Event và bắt đầu giữ hàng.
- Nhưng thực tế trong Database **không hề tồn tại đơn hàng**.

➡️ Các service khác đang xử lý **một đơn hàng "ma"**, dẫn đến dữ liệu sai lệch.

---

##### Đây gọi là Dual Write Problem

**Dual Write Problem** xảy ra khi một service phải **ghi dữ liệu vào hai nơi khác nhau** (ví dụ Database và Kafka), nhưng không thể đảm bảo cả hai đều thành công hoặc đều thất bại cùng lúc.

Đây là một trong những vấn đề phổ biến và nguy hiểm nhất khi xây dựng hệ thống microservices theo Event-Driven Architecture.

---

##### Giải pháp là gì?

Câu trả lời là **Outbox Pattern**.

Thay vì vừa lưu Database vừa gửi Kafka ngay lập tức, hệ thống sẽ:

1. Lưu dữ liệu nghiệp vụ vào Database.
2. Đồng thời lưu Event vào một bảng **Outbox** trong **cùng transaction**.
3. Sau đó, một tiến trình riêng sẽ đọc các Event trong Outbox và gửi chúng lên Kafka.

Nhờ vậy, Database và Event luôn được ghi **đồng thời trong một transaction**, tránh hoàn toàn lỗi **Dual Write Problem**.

---

## 9. Gợi ý thực hành

1. Vẽ ra giấy state machine đầy đủ cho flow `Order → Inventory → Payment → Shipping`, bao gồm tất cả happy path + 3 failure scenarios (stock không đủ, payment fail, shipping fail).
2. Implement 1 phiên bản Choreography với 4 Spring Boot service + Kafka, **chưa cần lo Outbox/Idempotency** (đó là bài sau).
3. Implement 1 phiên bản Orchestration với cùng 4 service + 1 SagaOrchestrator service.
4. Đo trải nghiệm: cái nào dễ debug hơn? Cái nào dễ extend hơn?
5. Đọc paper gốc: **"Sagas" (Garcia-Molina, Salem 1987)** — chỉ 9 trang.
6. Đọc chương 4 và 6 trong sách **"Microservices Patterns"** của Chris Richardson — bible về Saga.
7. Xem Temporal.io tutorials — nếu sau này hệ thống lớn, Temporal là lựa chọn rất mạnh.

---

## Checklist tự đánh giá

- [ ] Định nghĩa Saga, phân biệt với 2PC
- [ ] Giải thích Compensating Transaction, đặc tính bắt buộc
- [ ] Phân biệt Backward vs Forward Recovery
- [ ] Vẽ được flow Choreography đầy đủ + failure scenarios
- [ ] Vẽ được flow Orchestration đầy đủ + state machine
- [ ] So sánh được ưu/nhược điểm 2 biến thể, biết khi nào chọn cái nào
- [ ] Hiểu vì sao Saga không có Isolation, hệ quả với UI/UX
- [ ] Liệt kê 7 bẫy phổ biến khi implement Saga
- [ ] Biết các framework Saga (Axon, Eventuate, Temporal, Camunda)
- [ ] Trả lời được: "Trong flow này, nên dùng Choreography hay Orchestration? Vì sao?"

---

**Bài tiếp theo:** `05 - Outbox Pattern: giải quyết Dual Write Problem` — pattern cốt lõi giữ cho Saga hoạt động đúng trong thực tế.
