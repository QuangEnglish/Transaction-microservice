# Saga Pattern: Orchestration vs Choreography

> Bài 4 trong lộ trình Transaction Microservice nâng cao.
> Mục tiêu: Hiểu sâu Saga Pattern — cách industry hiện đại quản lý transaction xuyên nhiều microservice. Phân biệt 2 biến thể Orchestration và Choreography, biết khi nào dùng cái nào, và cách áp dụng vào flow `Order → Inventory → Payment → Shipping`.

---

## 0. Nhắc lại context

- **Bài 1-2:** ACID không thể trải dài qua nhiều DB. Microservices buộc phải chọn Eventual Consistency.
- **Bài 3:** 2PC/XA tồn tại nhưng blocking, không scale, không hợp Kafka/REST → industry bỏ.
- **Bài 4 (bài này):** **Saga** là gì, hoạt động ra sao, làm sao thay thế được distributed transaction.

---

## 1. Saga là gì

### 1.1. Định nghĩa

> **Saga = một chuỗi các local transaction**, mỗi transaction cập nhật 1 service. Nếu một bước fail, Saga thực hiện **các compensating transaction** để undo các bước trước đó.

Khái niệm gốc từ paper **"Sagas"** của Hector Garcia-Molina & Kenneth Salem (1987), ban đầu dành cho long-lived transaction trong 1 DB. Sau này được áp dụng vào microservices.

### 1.2. Ý tưởng cốt lõi

Thay vì giữ lock toàn flow (như 2PC):
1. Mỗi service thực hiện **local transaction ACID** của riêng nó → commit ngay
2. Sau khi commit, **publish event/command** để bước tiếp theo bắt đầu
3. Nếu bước nào fail → **trigger compensating transaction** ngược về trước

→ Đánh đổi:
- **Mất:** Isolation (giai đoạn giữa flow có thể đọc inconsistent)
- **Mất:** Atomicity strict (không phải all-or-nothing tức thì)
- **Được:** Availability, scalability, loose coupling

### 1.3. Compensating Transaction — Khái niệm cốt lõi

> **Compensating transaction** = một transaction **mới** thực hiện hành động **semantically undo** một transaction trước đó.

Lưu ý: KHÔNG phải rollback.
- **Rollback** (ACID): xoá vết, như chưa từng có
- **Compensation**: thêm 1 transaction ngược lại, **để lại lịch sử**

#### Ví dụ ngân hàng
```
T1: Trừ 100 từ A → A: 900 (đã commit)
T2: Cộng 100 vào B → fail!
C1: Cộng 100 lại vào A → A: 1000
```
Trong audit log: có 3 transaction (trừ, cộng fail, cộng bù). Không phải "như chưa từng có".

#### Đặc tính bắt buộc của Compensating Transaction
- **Idempotent**: chạy nhiều lần kết quả như 1 lần (sẽ học kỹ ở bài 6)
- **Commutative** (tốt nhất): thứ tự không ảnh hưởng
- **Semantically correct**: phản ánh đúng business intent
- **Không cần thiết phải hoàn nguyên 100% trạng thái cũ** (impossible với một số case, ví dụ "email đã gửi đi rồi")

#### Compensation không phải lúc nào cũng làm được
- Email đã gửi → không thể "ungửi". Phải gửi email apology.
- Payment đã capture → có thể refund (compensation), nhưng có phí
- Inventory đã ship → recall vật lý, không có "undo" trong code

→ Senior phải design business flow để **compensation luôn khả thi**.

---

## 2. Anatomy của một Saga

### 2.1. Cấu trúc

Một Saga gồm:
- **Saga Definition**: chuỗi `T1 → T2 → T3 → ... → Tn` và compensation tương ứng `C1, C2, ..., Cn-1`
- **Saga Execution Coordinator (SEC)**: ai điều phối các bước (centralized hoặc distributed)
- **Saga Log**: ghi lại trạng thái từng bước (để recovery khi crash)

### 2.2. Forward Recovery vs Backward Recovery

**Backward Recovery (phổ biến hơn)**
- Nếu Ti fail → chạy Ci-1, Ci-2, ..., C1 theo thứ tự ngược
- Áp dụng khi có thể compensate
- Ví dụ: payment fail → refund, release inventory, cancel order

**Forward Recovery (Pure Saga)**
- Nếu Ti fail → retry Ti cho đến khi thành công
- Áp dụng khi **bắt buộc** phải đi đến cuối (ví dụ regulatory)
- Cần có cơ chế retry và human escalation
- Ví dụ: tax filing — phải nộp được, không có "không nộp"

**Mixed**
- Một số bước backward, một số bước forward
- Thực tế thường gặp

### 2.3. Saga States

```
PENDING → IN_PROGRESS → COMPLETED
                ↓
            FAILED → COMPENSATING → COMPENSATED
                                   ↓
                          COMPENSATION_FAILED (cần human intervention)
```

---

## 3. Hai biến thể: Choreography vs Orchestration

Đây là **lựa chọn quan trọng nhất** khi design Saga.

### 3.1. Choreography — "Tự biên tự diễn"

> Không có coordinator trung tâm. Mỗi service **lắng nghe event** và **publish event mới** khi xong việc.

#### Sơ đồ flow `Order → Inventory → Payment → Shipping`

```
OrderService                Kafka              InventoryService    PaymentService    ShippingService
     |                        |                      |                  |                 |
     | INSERT order (PENDING) |                      |                  |                 |
     | publish OrderCreated   |--------------------->| consume          |                 |
     |                        |                      | reserve stock    |                 |
     |                        |                      | publish StockReserved             |
     |                        |<---------------------|                  |                 |
     |                        |---------------------------------------->| consume         |
     |                        |                      |                  | charge card     |
     |                        |                      |                  | publish PaymentCompleted
     |                        |<---------------------------------------|                  |
     |                        |--------------------------------------------------------->|
     |                        |                      |                  |                 | ship
     |                        |                      |                  |                 | publish OrderShipped
     |                        |<-----------------------------------------------------------|
     | consume                |                      |                  |                 |
     | update order=COMPLETED |                      |                  |                 |
```

#### Khi có lỗi (ví dụ Payment fail)
```
PaymentService publish PaymentFailed
    ↓
InventoryService consume PaymentFailed → release stock → publish StockReleased
    ↓
OrderService consume StockReleased → update order=CANCELLED
```

#### Ưu điểm
- **Loose coupling**: services không biết về nhau, chỉ biết events
- **Không single point of failure**: không có coordinator
- **Dễ thêm service mới**: service mới subscribe events, không sửa ai cả
- **Phù hợp flow đơn giản**: 3-4 bước

#### Nhược điểm
- **Khó debug, khó trace**: logic flow **không nằm ở đâu cả**, nằm rải rác trong từng consumer
- **Khó hiểu big picture**: muốn biết toàn flow phải đọc code của tất cả service
- **Cyclic dependency risk**: A listen B, B listen A → loop dễ xảy ra
- **Khó implement compensation logic phức tạp**: ai biết bước nào đã chạy, bước nào chưa?
- **Khó test end-to-end**: phải mock toàn bộ event bus

#### Khi nào dùng Choreography
- Flow đơn giản (≤ 4 bước)
- Domain rõ ràng, ít branching
- Team có culture event-driven mạnh
- Cần loose coupling tối đa
- Ví dụ điển hình: user signup flow, order notification

### 3.2. Orchestration — "Có nhạc trưởng"

> Có một **Saga Orchestrator** (thường là một service riêng) điều phối toàn bộ flow. Nó gửi **command** đến các service và nhận **reply**.

#### Sơ đồ flow `Order → Inventory → Payment → Shipping`

```
                          OrderSagaOrchestrator
                                  |
                ┌─────────────────┼──────────────────┬──────────────┐
                ↓                 ↓                  ↓              ↓
        OrderService     InventoryService    PaymentService   ShippingService
```

Flow:
```
1. Client → OrderService: createOrder()
2. OrderService → SagaOrchestrator: start Saga
3. Orchestrator: state = ORDER_CREATED
4. Orchestrator → InventoryService: ReserveStockCommand
5. InventoryService → Orchestrator: StockReservedReply
6. Orchestrator: state = STOCK_RESERVED
7. Orchestrator → PaymentService: ChargePaymentCommand
8. PaymentService → Orchestrator: PaymentCompletedReply (hoặc PaymentFailedReply)
9. Nếu OK: Orchestrator → ShippingService: ShipOrderCommand
10. ShippingService → Orchestrator: OrderShippedReply
11. Orchestrator: state = COMPLETED
```

#### Khi có lỗi (Payment fail)
```
8'. PaymentService → Orchestrator: PaymentFailedReply
9'. Orchestrator: state = COMPENSATING
10'. Orchestrator → InventoryService: ReleaseStockCommand
11'. InventoryService → Orchestrator: StockReleasedReply
12'. Orchestrator → OrderService: CancelOrderCommand
13'. Orchestrator: state = COMPENSATED
```

#### Communication: Command/Reply qua message broker

Orchestrator giao tiếp qua **2 loại message**:
- **Command** (Orchestrator → Service): "Hãy làm việc X"
- **Reply** (Service → Orchestrator): "Đã làm xong" / "Đã fail"

Thường dùng Kafka với:
- Topic `order-saga-commands` (Orchestrator publish)
- Topic `order-saga-replies` (Services publish)

Hoặc dùng RPC trực tiếp (gRPC) trong một số design.

#### Ưu điểm
- **Logic tập trung**: muốn hiểu flow → đọc 1 file Orchestrator
- **Dễ test, dễ trace**: có 1 state machine rõ ràng
- **Compensation logic rõ ràng**: orchestrator biết đã đến bước nào
- **Phù hợp flow phức tạp**: nhiều nhánh, nhiều điều kiện
- **Dễ thêm bước mới**: chỉ sửa Orchestrator

#### Nhược điểm
- **Single point of failure** (cần HA cho Orchestrator)
- **Tight coupling logic**: Orchestrator biết về tất cả service
- **Risk anti-pattern "God service"**: Orchestrator phình to chứa cả business logic
- **Cần cẩn thận distributed monolith**: Orchestrator + services deploy phải đồng bộ

#### Khi nào dùng Orchestration
- Flow ≥ 4 bước
- Có nhiều nhánh điều kiện (if-else, retry, timeout)
- Compensation logic phức tạp
- Cần observability cao (audit, monitoring)
- Ví dụ điển hình: e-commerce checkout, loan application, KYC flow

### 3.3. So sánh trực diện

| Tiêu chí | Choreography | Orchestration |
|---|---|---|
| Coordinator | Không có | Có (Orchestrator service) |
| Coupling | Lỏng (qua events) | Chặt hơn (Orchestrator biết tất) |
| Big picture | Khó nhìn | Dễ nhìn (1 nơi) |
| Debug | Khó | Dễ hơn |
| Phù hợp flow | Đơn giản | Phức tạp |
| Cyclic risk | Có | Không |
| Single point of failure | Không | Có (cần HA) |
| Thêm bước mới | Sửa nhiều service | Sửa Orchestrator |
| Tooling | Kafka + listeners | + State machine framework |
| Anti-pattern | Spaghetti events | God service |
| Test E2E | Khó | Dễ hơn |

### 3.4. Trong thực tế: dùng cả hai

Nhiều hệ thống lớn dùng **hybrid**:
- Orchestration cho **core flow** (checkout, payment)
- Choreography cho **side effects** (notification, analytics, audit)

Ví dụ:
```
Orchestrator quản lý: Order → Inventory → Payment → Shipping
       ↓ (after COMPLETED, publish event)
   "OrderCompleted" → NotificationService (choreography)
                    → AnalyticsService (choreography)
                    → LoyaltyService (choreography)
```

---

## 4. Saga thực hiện flow của bạn — Thiết kế chi tiết

### 4.1. Flow happy path

```
1. OrderService:
   - INSERT order (status=PENDING)
   - Save event "OrderCreated" (outbox - học bài 5)

2. InventoryService:
   - Receive command/event "ReserveStock"
   - UPDATE inventory SET reserved += qty WHERE product_id=X
   - Save event "StockReserved"

3. PaymentService:
   - Receive "ChargePayment"
   - Call payment gateway → get transaction_id
   - INSERT payment record
   - Save event "PaymentCompleted"

4. ShippingService:
   - Receive "CreateShipment"
   - INSERT shipment, call carrier API
   - Save event "ShipmentCreated"

5. OrderService:
   - Receive "ShipmentCreated"
   - UPDATE order SET status=CONFIRMED
```

### 4.2. Flow lỗi tại Payment (compensation backward)

```
3'. PaymentService:
    - Call gateway → DECLINED
    - INSERT payment (status=FAILED)
    - Save event "PaymentFailed"

4'. (Compensation) InventoryService:
    - Receive "ReleaseStock"
    - UPDATE inventory SET reserved -= qty
    - Save event "StockReleased"

5'. (Compensation) OrderService:
    - Receive "OrderCancelled"
    - UPDATE order SET status=CANCELLED
```

### 4.3. Phân chia bảng (theo service)

```
Order Service DB:
  - orders (id, customer_id, status, total, created_at)
  - order_items
  - outbox_events (sẽ học bài 5)

Inventory Service DB:
  - products
  - inventory (product_id, available, reserved)
  - stock_movements (audit)

Payment Service DB:
  - payments (id, order_id, amount, status, gateway_txn_id)
  - payment_attempts

Shipping Service DB:
  - shipments (id, order_id, carrier, tracking_no, status)
```

### 4.4. Order States (Orchestrator state machine)

```
NEW → STOCK_RESERVING → STOCK_RESERVED 
                      ↘ STOCK_RESERVE_FAILED → FAILED

STOCK_RESERVED → PAYMENT_CHARGING → PAYMENT_COMPLETED
                                 ↘ PAYMENT_FAILED → COMPENSATING_STOCK → CANCELLED

PAYMENT_COMPLETED → SHIPMENT_CREATING → SHIPPED
                                      ↘ SHIPMENT_FAILED → COMPENSATING_PAYMENT (refund)
                                                       → COMPENSATING_STOCK
                                                       → CANCELLED

SHIPPED → COMPLETED (terminal state)
```

---

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

### 5.3. Cảnh báo: code mẫu trên CHƯA HOÀN CHỈNH

Có vài vấn đề mà code đơn giản ở trên **bỏ qua**, nhưng production bắt buộc phải xử lý:

1. **Dual Write Problem**: `orderRepository.save()` + `kafkaTemplate.send()` — nếu 1 fail thì sao? → **Outbox Pattern** (bài 5)
2. **Idempotency**: Kafka at-least-once, message có thể bị deliver nhiều lần → **Idempotent Consumer** (bài 6)
3. **Ordering**: events đến không đúng thứ tự thì sao? → partition key + version
4. **Retry & Dead Letter Queue**: khi consumer fail → retry rồi cuối cùng dump vào DLQ

→ Các bài sau sẽ giải quyết từng vấn đề.

---

## 6. Khi nào KHÔNG dùng Saga

Saga rất mạnh nhưng không phải bạc đạn. **Không dùng** khi:

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

Bạn đã hiểu Saga ở mức conceptual + skeleton code. Nhưng có 1 vấn đề thực tế **chí mạng** chưa giải:

> Làm sao để `orderRepository.save()` và `kafkaTemplate.send()` được thực hiện **atomically**?

Nếu DB save OK mà Kafka send fail → DB có order, không ai biết → Saga ngưng đó.
Nếu DB save fail mà Kafka đã send → các service khác xử lý 1 order ma → catastrophe.

→ Đây là **Dual Write Problem**, và lời giải là **Outbox Pattern** — bài tiếp theo.

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
