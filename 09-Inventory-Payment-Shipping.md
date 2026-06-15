# Implement Inventory, Payment, Shipping Services

> Bài 9 trong lộ trình Transaction Microservice nâng cao.
> Mục tiêu: Code 3 service còn lại theo cùng pattern Order Service. Tập trung vào **business logic đặc thù** của từng domain — Reservation Pattern (Inventory), Payment Gateway Integration (Payment), Carrier API (Shipping). Skip boilerplate đã có ở bài 8.

---

## 0. Vai trò 3 service trong Saga

```
                Saga Orchestrator
                       │
        ┌──────────────┼──────────────┐
        │              │              │
        ▼              ▼              ▼
  Inventory        Payment         Shipping
   Service         Service         Service

  reserve()    →   charge()    →  createShipment()
   ↑                ↑                ↑
   │ release()      │ refund()       │ cancelShipment()
   │                │                │
   └─ Compensation ─┴─ Compensation ─┘
```

| Service | Forward | Compensation | Đặc trưng |
|---|---|---|---|
| **Inventory** | Reserve stock | Release stock | Concurrent access cao, optimistic lock |
| **Payment** | Charge card | Refund | Tích hợp external gateway, idempotency-key |
| **Shipping** | Create shipment | Cancel shipment | Tích hợp carrier API, late compensation khó |

→ Mỗi service đều có cùng **template kỹ thuật**: REST API + Outbox + Inbox + Command listener. Khác nhau ở **domain logic**.

---

## 1. Inventory Service

### 1.1. Reservation Pattern — Khái niệm cốt lõi

**Vấn đề:** Khi order tạo, không thể "trừ hẳn" stock ngay vì Saga có thể fail (payment decline). Nhưng cũng không thể "không làm gì" vì user khác có thể mua hết hàng trước khi Saga xong.

**Giải pháp: Reservation (Soft hold)**

```
Inventory state:
  available_quantity:  100   ← user thấy con số này khi browse
  reserved_quantity:    15   ← đã reserve cho các order đang xử lý
  effective_available: 100 - 15 = 85
```

Flow:
1. **Reserve** (lúc Saga bắt đầu): `available -= qty, reserved += qty` (hoặc model khác: chỉ tăng `reserved`)
2. **Commit** (lúc Saga thành công): `reserved -= qty` (release reservation, stock đã thực sự bán)
3. **Release** (compensation, lúc Saga fail): `available += qty, reserved -= qty` (trả về kho)

→ User khác query `available` → chỉ thấy hàng "thực sự còn lại". Không bị oversell.

### 1.2. Schema

`inventory-service/.../V002__create_inventory_tables.sql`:
```sql
CREATE TABLE products (
    id           VARCHAR(100) PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    description  TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE inventory (
    product_id          VARCHAR(100) PRIMARY KEY REFERENCES products(id),
    available_quantity  INT NOT NULL,
    reserved_quantity   INT NOT NULL DEFAULT 0,
    version             INT NOT NULL DEFAULT 0,           -- optimistic lock
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT non_negative_available CHECK (available_quantity >= 0),
    CONSTRAINT non_negative_reserved CHECK (reserved_quantity >= 0)
);

CREATE TABLE stock_reservations (
    id              UUID PRIMARY KEY,
    order_id        UUID NOT NULL,                        -- 1 reservation = 1 order
    saga_id         UUID NOT NULL,
    product_id      VARCHAR(100) NOT NULL REFERENCES products(id),
    quantity        INT NOT NULL,
    status          VARCHAR(50) NOT NULL,                 -- RESERVED, COMMITTED, RELEASED
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL,                 -- TTL: nếu Saga timeout, auto-release
    
    UNIQUE (order_id, product_id)                         -- 1 order x 1 product = 1 reservation
);

CREATE INDEX idx_reservations_status ON stock_reservations(status);
CREATE INDEX idx_reservations_saga_id ON stock_reservations(saga_id);
CREATE INDEX idx_reservations_expires ON stock_reservations(expires_at) 
    WHERE status = 'RESERVED';                            -- partial index cho cleanup job
```

**Lưu ý quan trọng về UNIQUE constraint:**
- `UNIQUE(order_id, product_id)` đảm bảo: 1 order chỉ reserve được 1 lần cho cùng product
- Nếu command duplicate đến → INSERT thứ 2 fail vì UNIQUE → safe (kết hợp với Inbox)
- → **Natural idempotency** ở DB level

### 1.3. Domain Entities

`com/demo/inventory/domain/Inventory.java`:
```java
@Entity
@Table(name = "inventory")
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Inventory {

    @Id
    @Column(name = "product_id")
    private String productId;

    @Column(name = "available_quantity", nullable = false)
    private Integer availableQuantity;

    @Column(name = "reserved_quantity", nullable = false)
    private Integer reservedQuantity;

    @Setter
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Version
    private Integer version;

    /**
     * Reserve stock. Throw nếu không đủ.
     * Business logic ở Entity — DDD style.
     */
    public void reserve(int quantity) {
        if (availableQuantity < quantity) {
            throw new InsufficientStockException(
                productId, quantity, availableQuantity);
        }
        this.availableQuantity -= quantity;
        this.reservedQuantity += quantity;
        this.updatedAt = Instant.now();
    }

    /**
     * Commit reservation. Stock đã thực sự bán → giảm reserved.
     */
    public void commitReservation(int quantity) {
        if (reservedQuantity < quantity) {
            throw new IllegalStateException(
                "Cannot commit more than reserved");
        }
        this.reservedQuantity -= quantity;
        this.updatedAt = Instant.now();
    }

    /**
     * Release reservation. Compensation — trả về available.
     */
    public void release(int quantity) {
        if (reservedQuantity < quantity) {
            throw new IllegalStateException(
                "Cannot release more than reserved");
        }
        this.reservedQuantity -= quantity;
        this.availableQuantity += quantity;
        this.updatedAt = Instant.now();
    }

    public int effectiveAvailable() {
        return availableQuantity;     // reserved đã trừ rồi
    }
}
```

`com/demo/inventory/domain/StockReservation.java`:
```java
@Entity
@Table(name = "stock_reservations")
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StockReservation {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "saga_id", nullable = false)
    private UUID sagaId;

    @Column(name = "product_id", nullable = false)
    private String productId;

    @Column(nullable = false)
    private Integer quantity;

    @Enumerated(EnumType.STRING)
    @Setter
    @Column(nullable = false, length = 50)
    private ReservationStatus status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "expires_at", nullable = false)
    private Instant expiresAt;

    public void commit() {
        if (status != ReservationStatus.RESERVED) {
            throw new IllegalStateException(
                "Cannot commit reservation in status " + status);
        }
        this.status = ReservationStatus.COMMITTED;
    }

    public void release() {
        if (status == ReservationStatus.RELEASED) return;  // idempotent
        if (status == ReservationStatus.COMMITTED) {
            throw new IllegalStateException(
                "Cannot release committed reservation");
        }
        this.status = ReservationStatus.RELEASED;
    }

    public boolean isExpired() {
        return Instant.now().isAfter(expiresAt);
    }
}

public enum ReservationStatus {
    RESERVED,    // Đã reserve, chờ Saga xử lý
    COMMITTED,   // Saga thành công, stock đã bán
    RELEASED     // Compensation, trả về available
}
```

### 1.4. Application Service

`com/demo/inventory/application/InventoryService.java`:
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class InventoryService {

    private final InventoryRepository inventoryRepository;
    private final ReservationRepository reservationRepository;

    private static final Duration RESERVATION_TTL = Duration.ofMinutes(15);

    /**
     * Reserve stock cho 1 order item. 
     * Idempotent qua UNIQUE(order_id, product_id) ở DB.
     */
    @Transactional
    public StockReservation reserve(UUID orderId, UUID sagaId, 
                                     String productId, int quantity) {
        // 1. Check reservation đã tồn tại chưa (idempotency thứ 2)
        Optional<StockReservation> existing = reservationRepository
            .findByOrderIdAndProductId(orderId, productId);
        if (existing.isPresent()) {
            log.info("Reservation already exists for order={}, product={}, status={}",
                orderId, productId, existing.get().getStatus());
            return existing.get();
        }

        // 2. Load inventory với optimistic lock
        Inventory inventory = inventoryRepository.findById(productId)
            .orElseThrow(() -> new ProductNotFoundException(productId));

        // 3. Reserve (entity tự throw nếu không đủ)
        inventory.reserve(quantity);
        inventoryRepository.save(inventory);

        // 4. Tạo reservation record
        StockReservation reservation = StockReservation.builder()
            .id(UUID.randomUUID())
            .orderId(orderId)
            .sagaId(sagaId)
            .productId(productId)
            .quantity(quantity)
            .status(ReservationStatus.RESERVED)
            .createdAt(Instant.now())
            .expiresAt(Instant.now().plus(RESERVATION_TTL))
            .build();
            
        reservationRepository.save(reservation);
        log.info("Stock reserved: orderId={}, product={}, qty={}, available={}",
            orderId, productId, quantity, inventory.getAvailableQuantity());

        return reservation;
    }

    /**
     * Release reservation (compensation).
     * Idempotent qua reservation status check.
     */
    @Transactional
    public void release(UUID orderId, String productId) {
        StockReservation reservation = reservationRepository
            .findByOrderIdAndProductId(orderId, productId)
            .orElse(null);

        if (reservation == null) {
            log.warn("No reservation found for order={}, product={}, skipping",
                orderId, productId);
            return;  // idempotent: không có gì để release
        }

        if (reservation.getStatus() == ReservationStatus.RELEASED) {
            log.info("Reservation {} already released", reservation.getId());
            return;
        }

        if (reservation.getStatus() == ReservationStatus.COMMITTED) {
            // Edge case: Saga đã commit, giờ mới yêu cầu release?
            // Nghĩa là compensation đến muộn → cần báo lỗi, không tự ý undo
            log.error("Cannot release committed reservation {}", reservation.getId());
            throw new IllegalStateException("Reservation already committed");
        }

        Inventory inventory = inventoryRepository.findById(productId).orElseThrow();
        inventory.release(reservation.getQuantity());
        inventoryRepository.save(inventory);

        reservation.release();
        reservationRepository.save(reservation);

        log.info("Reservation released: orderId={}, product={}, qty={}",
            orderId, productId, reservation.getQuantity());
    }

    /**
     * Commit reservation (Saga thành công).
     * Stock đã thực sự bán → giảm reserved.
     */
    @Transactional
    public void commit(UUID orderId, String productId) {
        StockReservation reservation = reservationRepository
            .findByOrderIdAndProductId(orderId, productId)
            .orElseThrow(() -> new ReservationNotFoundException(orderId, productId));

        if (reservation.getStatus() == ReservationStatus.COMMITTED) {
            return;  // idempotent
        }

        Inventory inventory = inventoryRepository.findById(productId).orElseThrow();
        inventory.commitReservation(reservation.getQuantity());
        inventoryRepository.save(inventory);

        reservation.commit();
        reservationRepository.save(reservation);
    }
}
```

### 1.5. SagaCommandHandler cho Inventory

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class SagaCommandHandler {

    private final InventoryService inventoryService;
    private final OutboxPublisher outboxPublisher;
    private final IdempotentConsumer idempotentConsumer;

    private static final String CONSUMER_GROUP = "inventory-service.saga-handler";
    private static final String REPLIES_TOPIC = "saga.replies";

    @Transactional
    public void handleReserveStock(ReserveStockCommand command) {
        idempotentConsumer.processOnce(
            command.getCommandId(),
            CONSUMER_GROUP,
            () -> doReserve(command)
        );
    }

    private void doReserve(ReserveStockCommand command) {
        UUID orderId = UUID.fromString(command.getOrderId());
        UUID sagaId = UUID.fromString(command.getSagaId());

        try {
            // Reserve tất cả items trong order
            for (OrderItemInfo item : command.getItems()) {
                inventoryService.reserve(orderId, sagaId, 
                    item.getProductId(), item.getQuantity());
            }
            
            sendReply(command, true, null);
            
        } catch (InsufficientStockException e) {
            log.warn("Insufficient stock for saga={}: {}", sagaId, e.getMessage());
            
            // Rollback các reservation đã tạo trong vòng lặp (nếu có)
            // Vì @Transactional → tất cả rollback tự động khi throw
            // Nhưng ta đã catch nên không throw → cần manual cleanup
            // → Hoặc: throw lại để rollback, rồi reply riêng
            
            sendReply(command, false, e.getMessage());
        } catch (Exception e) {
            log.error("Failed to reserve stock for saga={}", sagaId, e);
            sendReply(command, false, "Internal error: " + e.getMessage());
        }
    }

    @Transactional
    public void handleReleaseStock(ReleaseStockCommand command) {
        idempotentConsumer.processOnce(
            command.getCommandId(),
            CONSUMER_GROUP,
            () -> doRelease(command)
        );
    }

    private void doRelease(ReleaseStockCommand command) {
        UUID orderId = UUID.fromString(command.getOrderId());

        try {
            for (OrderItemInfo item : command.getItems()) {
                inventoryService.release(orderId, item.getProductId());
            }
            sendReply(command, true, null);
        } catch (Exception e) {
            log.error("Failed to release stock", e);
            sendReply(command, false, e.getMessage());
        }
    }

    private void sendReply(SagaCommand command, boolean success, String reason) {
        InventoryCommandReply reply = InventoryCommandReply.builder()
            .replyId(UUID.randomUUID())
            .commandId(command.getCommandId())
            .sagaId(command.getSagaId())
            .success(success)
            .failureReason(reason)
            .build();

        outboxPublisher.publishCommand(reply, REPLIES_TOPIC, command.getSagaId());
    }
}
```

### 1.6. Cleanup expired reservations (cron job)

```java
@Component
@RequiredArgsConstructor
@Slf4j
public class ReservationCleanupJob {

    private final ReservationRepository reservationRepository;
    private final InventoryService inventoryService;

    /**
     * Mỗi 5 phút, release các reservation đã expire (Saga có lẽ đã chết giữa chừng).
     * Đây là "fallback safety net" — không thay thế compensation chính.
     */
    @Scheduled(fixedDelay = 5 * 60 * 1000)
    public void releaseExpiredReservations() {
        List<StockReservation> expired = reservationRepository
            .findExpiredReserved(Instant.now());

        log.info("Found {} expired reservations to release", expired.size());

        for (StockReservation r : expired) {
            try {
                inventoryService.release(r.getOrderId(), r.getProductId());
                log.info("Released expired reservation {}", r.getId());
            } catch (Exception e) {
                log.error("Failed to release expired reservation {}", r.getId(), e);
            }
        }
    }
}
```

→ **Đây là pattern "Sweeper"** — backup safety net cho Saga timeout. Không phải thay thế compensation, mà là **catch all** cho các case Saga "biến mất" giữa chừng.

---

## 2. Payment Service

### 2.1. Payment Gateway Integration — Patterns

Khi tích hợp với payment gateway thật (Stripe, Adyen, VNPay, Momo), có các pattern quan trọng:

**Pattern 1: Idempotency-Key (BẮT BUỘC)**
Mọi payment API thật đều support `Idempotency-Key`:
```http
POST /v1/charges
Idempotency-Key: <unique_per_attempt>
```
Gateway đảm bảo cùng key chỉ charge 1 lần.

**Pattern 2: Webhook callback**
Một số payment async: gateway return `pending`, sau đó callback webhook khi xong.
→ Service phải handle 2 case: response trực tiếp và webhook.

**Pattern 3: Reconciliation**
Cuối ngày so sánh DB internal với report từ gateway → tìm discrepancy.

**Pattern 4: PCI-DSS compliance**
Không bao giờ lưu raw card data. Lưu `token` từ gateway.

→ Demo này dùng **mock gateway** với delay và random fail để simulate. Production thay bằng thật.

### 2.2. Schema

```sql
CREATE TABLE payments (
    id                  UUID PRIMARY KEY,
    order_id            UUID NOT NULL,
    saga_id             UUID NOT NULL,
    customer_id         VARCHAR(100) NOT NULL,
    amount              DECIMAL(19,4) NOT NULL,
    currency            VARCHAR(3) NOT NULL DEFAULT 'USD',
    status              VARCHAR(50) NOT NULL,            -- PENDING, COMPLETED, FAILED, REFUNDED
    gateway_txn_id      VARCHAR(255),                    -- transaction ID từ gateway
    idempotency_key     VARCHAR(100) NOT NULL UNIQUE,    -- chính ta sinh
    failure_reason      TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    version             INT NOT NULL DEFAULT 0,
    
    UNIQUE (order_id)                                    -- 1 order chỉ có 1 payment
);

CREATE TABLE payment_attempts (
    id              UUID PRIMARY KEY,
    payment_id      UUID NOT NULL REFERENCES payments(id),
    attempt_no      INT NOT NULL,
    status          VARCHAR(50) NOT NULL,                -- SUCCESS, FAILED
    gateway_response JSONB,                              -- log response để debug
    attempted_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    
    UNIQUE (payment_id, attempt_no)
);

CREATE TABLE refunds (
    id              UUID PRIMARY KEY,
    payment_id      UUID NOT NULL REFERENCES payments(id),
    amount          DECIMAL(19,4) NOT NULL,
    status          VARCHAR(50) NOT NULL,                -- PENDING, COMPLETED, FAILED
    gateway_refund_id VARCHAR(255),
    reason          TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### 2.3. Mock Payment Gateway

```java
@Component
@Slf4j
public class MockPaymentGateway {

    private final Random random = new Random();

    @Value("${payment.gateway.failure-rate:0.1}")
    private double failureRate;

    @Value("${payment.gateway.latency-ms:200}")
    private long latencyMs;

    public GatewayChargeResult charge(GatewayChargeRequest request) {
        log.info("Calling gateway: idempotencyKey={}, amount={}",
            request.getIdempotencyKey(), request.getAmount());

        // Simulate network latency
        sleep(latencyMs);

        // Idempotency check (gateway thật làm phía nó)
        // Demo: store in-memory map nếu cần test repeat call
        
        // Simulate random failure
        if (random.nextDouble() < failureRate) {
            String[] reasons = {"INSUFFICIENT_FUNDS", "CARD_DECLINED", "EXPIRED_CARD"};
            String reason = reasons[random.nextInt(reasons.length)];
            return GatewayChargeResult.builder()
                .success(false)
                .failureReason(reason)
                .build();
        }

        return GatewayChargeResult.builder()
            .success(true)
            .gatewayTxnId("TXN-" + UUID.randomUUID())
            .build();
    }

    public GatewayRefundResult refund(GatewayRefundRequest request) {
        sleep(latencyMs);
        // Refund hiếm fail (giả định)
        return GatewayRefundResult.builder()
            .success(true)
            .gatewayRefundId("RFD-" + UUID.randomUUID())
            .build();
    }

    private void sleep(long ms) {
        try { Thread.sleep(ms); } 
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
```

### 2.4. Payment Entity

```java
@Entity
@Table(name = "payments")
@Getter
@NoArgsConstructor @AllArgsConstructor @Builder
public class Payment {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "saga_id", nullable = false)
    private UUID sagaId;

    @Column(name = "customer_id", nullable = false)
    private String customerId;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Setter
    @Column(nullable = false, length = 50)
    private PaymentStatus status;

    @Setter
    @Column(name = "gateway_txn_id")
    private String gatewayTxnId;

    @Column(name = "idempotency_key", nullable = false, unique = true)
    private String idempotencyKey;

    @Setter
    @Column(name = "failure_reason")
    private String failureReason;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Setter
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Version
    private Integer version;

    public void markCompleted(String gatewayTxnId) {
        if (status != PaymentStatus.PENDING) {
            throw new IllegalStateException(
                "Cannot complete payment in status " + status);
        }
        this.status = PaymentStatus.COMPLETED;
        this.gatewayTxnId = gatewayTxnId;
        this.updatedAt = Instant.now();
    }

    public void markFailed(String reason) {
        if (status == PaymentStatus.COMPLETED) {
            throw new IllegalStateException(
                "Cannot fail completed payment");
        }
        this.status = PaymentStatus.FAILED;
        this.failureReason = reason;
        this.updatedAt = Instant.now();
    }

    public void markRefunded() {
        if (status != PaymentStatus.COMPLETED) {
            throw new IllegalStateException(
                "Can only refund completed payment, current status: " + status);
        }
        this.status = PaymentStatus.REFUNDED;
        this.updatedAt = Instant.now();
    }
}

public enum PaymentStatus {
    PENDING, COMPLETED, FAILED, REFUNDED
}
```

### 2.5. Payment Service

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final RefundRepository refundRepository;
    private final MockPaymentGateway gateway;

    @Transactional
    public Payment charge(UUID orderId, UUID sagaId, String customerId,
                          BigDecimal amount) {
        
        // 1. Check payment đã tồn tại chưa (idempotency)
        Optional<Payment> existing = paymentRepository.findByOrderId(orderId);
        if (existing.isPresent()) {
            log.info("Payment already exists for order={}, status={}",
                orderId, existing.get().getStatus());
            return existing.get();
        }

        // 2. Tạo payment PENDING
        String idempotencyKey = generateIdempotencyKey(orderId, sagaId);
        Payment payment = Payment.builder()
            .id(UUID.randomUUID())
            .orderId(orderId)
            .sagaId(sagaId)
            .customerId(customerId)
            .amount(amount)
            .currency("USD")
            .status(PaymentStatus.PENDING)
            .idempotencyKey(idempotencyKey)
            .createdAt(Instant.now())
            .updatedAt(Instant.now())
            .build();
        paymentRepository.save(payment);

        // 3. Call gateway
        GatewayChargeResult result = gateway.charge(GatewayChargeRequest.builder()
            .amount(amount)
            .currency("USD")
            .customerId(customerId)
            .idempotencyKey(idempotencyKey)
            .build());

        // 4. Update payment based on result
        if (result.isSuccess()) {
            payment.markCompleted(result.getGatewayTxnId());
            log.info("Payment completed: orderId={}, txnId={}", 
                orderId, result.getGatewayTxnId());
        } else {
            payment.markFailed(result.getFailureReason());
            log.warn("Payment failed: orderId={}, reason={}", 
                orderId, result.getFailureReason());
        }
        paymentRepository.save(payment);

        return payment;
    }

    @Transactional
    public Refund refund(UUID orderId, String reason) {
        Payment payment = paymentRepository.findByOrderId(orderId)
            .orElseThrow(() -> new PaymentNotFoundException(orderId));

        if (payment.getStatus() == PaymentStatus.REFUNDED) {
            log.info("Payment {} already refunded", payment.getId());
            return refundRepository.findByPaymentId(payment.getId())
                .orElseThrow();
        }

        if (payment.getStatus() != PaymentStatus.COMPLETED) {
            // Edge case: yêu cầu refund payment chưa complete (PENDING/FAILED)
            // Có thể là Saga gửi compensation cho payment đã fail
            // → đơn giản: nothing to refund
            log.info("Payment {} is in status {}, no refund needed", 
                payment.getId(), payment.getStatus());
            return null;
        }

        // Call gateway refund
        GatewayRefundResult result = gateway.refund(GatewayRefundRequest.builder()
            .gatewayTxnId(payment.getGatewayTxnId())
            .amount(payment.getAmount())
            .reason(reason)
            .build());

        if (!result.isSuccess()) {
            throw new RefundFailedException(
                "Gateway refund failed: " + result.getFailureReason());
        }

        // Create refund record
        Refund refund = Refund.builder()
            .id(UUID.randomUUID())
            .paymentId(payment.getId())
            .amount(payment.getAmount())
            .status(RefundStatus.COMPLETED)
            .gatewayRefundId(result.getGatewayRefundId())
            .reason(reason)
            .createdAt(Instant.now())
            .build();
        refundRepository.save(refund);

        payment.markRefunded();
        paymentRepository.save(payment);

        log.info("Payment refunded: paymentId={}, refundId={}",
            payment.getId(), result.getGatewayRefundId());

        return refund;
    }

    private String generateIdempotencyKey(UUID orderId, UUID sagaId) {
        // Đảm bảo unique nhưng deterministic: cùng saga + order luôn ra cùng key
        return "pay-" + orderId + "-" + sagaId;
    }
}
```

### 2.6. SagaCommandHandler cho Payment

```java
@Service
@RequiredArgsConstructor
public class SagaCommandHandler {
    // ...
    
    @Transactional
    public void handleChargePayment(ChargePaymentCommand command) {
        idempotentConsumer.processOnce(
            command.getCommandId(),
            CONSUMER_GROUP,
            () -> doCharge(command)
        );
    }

    private void doCharge(ChargePaymentCommand command) {
        try {
            Payment payment = paymentService.charge(
                UUID.fromString(command.getOrderId()),
                UUID.fromString(command.getSagaId()),
                command.getCustomerId(),
                command.getAmount()
            );

            boolean success = payment.getStatus() == PaymentStatus.COMPLETED;
            sendReply(command, success, 
                success ? null : payment.getFailureReason(),
                success ? payment.getGatewayTxnId() : null);

        } catch (Exception e) {
            log.error("Charge payment failed", e);
            sendReply(command, false, "Internal error: " + e.getMessage(), null);
        }
    }

    @Transactional
    public void handleRefundPayment(RefundPaymentCommand command) {
        idempotentConsumer.processOnce(
            command.getCommandId(),
            CONSUMER_GROUP,
            () -> doRefund(command)
        );
    }

    private void doRefund(RefundPaymentCommand command) {
        try {
            paymentService.refund(
                UUID.fromString(command.getOrderId()),
                command.getReason()
            );
            sendReply(command, true, null, null);
        } catch (Exception e) {
            log.error("Refund failed", e);
            sendReply(command, false, e.getMessage(), null);
        }
    }
}
```

---

## 3. Shipping Service

### 3.1. Đặc trưng

Shipping là service **cuối flow** — đơn giản hơn nhưng có 1 đặc thù: 

> **Compensation khó hơn các service khác.**

- Inventory: release stock = đảo bít số → dễ
- Payment: refund = call gateway → có tiền chi phí nhưng làm được
- **Shipping**: nếu carrier đã pickup hàng → recall vật lý → tốn kém, chậm, có khi không khả thi

→ Trong nhiều hệ thống thực, shipping được tách thành **2 bước**:
1. **CreateShipment** (lúc Saga đang chạy): tạo record DRAFT, chưa giao cho carrier
2. **ConfirmShipment** (sau khi Saga hoàn toàn xong): mới thực sự gửi tới carrier

→ Demo này giữ đơn giản: chỉ có 1 step CreateShipment. Cancel = đánh dấu CANCELLED, KHÔNG gọi carrier API recall.

### 3.2. Schema

```sql
CREATE TABLE shipments (
    id                  UUID PRIMARY KEY,
    order_id            UUID NOT NULL UNIQUE,
    saga_id             UUID NOT NULL,
    customer_id         VARCHAR(100) NOT NULL,
    shipping_address    JSONB NOT NULL,
    carrier             VARCHAR(50),                    -- "DHL", "FedEx", ...
    tracking_number     VARCHAR(100),
    status              VARCHAR(50) NOT NULL,           -- CREATED, IN_TRANSIT, DELIVERED, CANCELLED
    estimated_delivery  TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    version             INT NOT NULL DEFAULT 0
);

CREATE TABLE shipment_events (
    id              UUID PRIMARY KEY,
    shipment_id     UUID NOT NULL REFERENCES shipments(id),
    event_type      VARCHAR(50) NOT NULL,               -- PICKED_UP, IN_TRANSIT, DELIVERED, etc.
    location        VARCHAR(255),
    timestamp       TIMESTAMPTZ NOT NULL,
    payload         JSONB
);
```

### 3.3. Domain Entity

```java
@Entity
@Table(name = "shipments")
@Getter
@NoArgsConstructor @AllArgsConstructor @Builder
public class Shipment {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false, unique = true)
    private UUID orderId;

    @Column(name = "saga_id", nullable = false)
    private UUID sagaId;

    @Column(name = "customer_id", nullable = false)
    private String customerId;

    @Type(JsonType.class)
    @Column(name = "shipping_address", columnDefinition = "jsonb", nullable = false)
    private ShippingAddress shippingAddress;

    @Setter
    private String carrier;

    @Setter
    @Column(name = "tracking_number")
    private String trackingNumber;

    @Enumerated(EnumType.STRING)
    @Setter
    @Column(nullable = false, length = 50)
    private ShipmentStatus status;

    @Setter
    @Column(name = "estimated_delivery")
    private Instant estimatedDelivery;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Setter
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Version
    private Integer version;

    public void cancel() {
        if (status == ShipmentStatus.CANCELLED) return;
        if (status == ShipmentStatus.DELIVERED) {
            throw new IllegalStateException(
                "Cannot cancel delivered shipment");
        }
        if (status == ShipmentStatus.IN_TRANSIT) {
            // Realistic: cần call carrier API, có thể fail
            // Demo: chỉ đánh dấu
            log.warn("Cancelling shipment in transit - manual intervention needed");
        }
        this.status = ShipmentStatus.CANCELLED;
        this.updatedAt = Instant.now();
    }
}

public enum ShipmentStatus {
    CREATED, IN_TRANSIT, DELIVERED, CANCELLED
}
```

### 3.4. ShippingService

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class ShippingService {

    private final ShipmentRepository shipmentRepository;
    private final MockCarrierApi carrierApi;

    @Transactional
    public Shipment createShipment(UUID orderId, UUID sagaId, String customerId,
                                    ShippingAddress address) {
        
        // Idempotency: order đã có shipment chưa?
        Optional<Shipment> existing = shipmentRepository.findByOrderId(orderId);
        if (existing.isPresent()) {
            return existing.get();
        }

        // Call carrier API
        CarrierBookingResult booking = carrierApi.bookShipment(
            CarrierBookingRequest.builder()
                .orderId(orderId.toString())
                .address(address)
                .build());

        Shipment shipment = Shipment.builder()
            .id(UUID.randomUUID())
            .orderId(orderId)
            .sagaId(sagaId)
            .customerId(customerId)
            .shippingAddress(address)
            .carrier(booking.getCarrier())
            .trackingNumber(booking.getTrackingNumber())
            .status(ShipmentStatus.CREATED)
            .estimatedDelivery(booking.getEstimatedDelivery())
            .createdAt(Instant.now())
            .updatedAt(Instant.now())
            .build();
        
        shipmentRepository.save(shipment);
        log.info("Shipment created: orderId={}, tracking={}", 
            orderId, booking.getTrackingNumber());

        return shipment;
    }

    @Transactional
    public void cancelShipment(UUID orderId) {
        Shipment shipment = shipmentRepository.findByOrderId(orderId).orElse(null);
        if (shipment == null) {
            log.warn("No shipment found for order {}, nothing to cancel", orderId);
            return;
        }

        if (shipment.getStatus() == ShipmentStatus.CANCELLED) {
            return;  // idempotent
        }

        try {
            carrierApi.cancelBooking(shipment.getTrackingNumber());
        } catch (Exception e) {
            log.error("Carrier cancel failed for tracking={}, marking CANCELLED anyway",
                shipment.getTrackingNumber(), e);
            // Production: thường vẫn đánh dấu CANCELLED + alert ops team
        }

        shipment.cancel();
        shipmentRepository.save(shipment);
    }
}
```

---

## 4. Bẫy phổ biến — Specific cho từng domain

### Inventory

**Bẫy I-1: Race condition khi nhiều order cùng reserve product hot**

Optimistic lock fail → `OptimisticLockingFailureException`. Có 2 cách handle:

```java
// Cách 1: Retry với Spring Retry
@Retryable(
    retryFor = OptimisticLockingFailureException.class,
    maxAttempts = 5,
    backoff = @Backoff(delay = 50, multiplier = 2)
)
@Transactional
public void reserve(...) { ... }
```

```java
// Cách 2: Pessimistic lock (đơn giản nhưng giảm throughput)
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT i FROM Inventory i WHERE i.productId = :id")
Inventory findByIdForUpdate(@Param("id") String id);
```

Production: **Cách 1** thường ưu tiên (optimistic + retry), trừ khi contention cực cao.

**Bẫy I-2: Quên check `effective_available` ở read path**

User browse sản phẩm → query `available_quantity` → thấy 100 → mua → ra hết.
Vì `available - reserved` mới đúng. Giải pháp: view hoặc computed column.

**Bẫy I-3: Reservation TTL quá ngắn**

Đặt 1 phút → Saga chậm (gateway latency cao) → reservation expire giữa Saga → cleanup job release → Saga commit → conflict.
→ TTL phải dài hơn **worst-case Saga duration** + buffer.

### Payment

**Bẫy P-1: Quên Idempotency-Key**

Network timeout → retry charge → user bị charge 2 lần. **Mọi** gọi payment gateway PHẢI có idempotency-key.

**Bẫy P-2: Lưu raw card data**

PCI-DSS vi phạm = fine khổng lồ. Chỉ lưu token từ gateway.

**Bẫy P-3: Refund không thể thiếu`gateway_txn_id`**

Refund cần reference đến transaction gốc. Nếu service crash giữa charge và lưu txn_id → mất khả năng refund tự động.
→ Lưu txn_id ngay khi gateway trả về, kể cả nếu transaction sau đó rollback.

**Bẫy P-4: Refund 1 phần (partial refund)**

Compensation thường full refund. Nhưng business có thể yêu cầu partial (ví dụ user đã nhận 1 trong 3 món). Schema cần support `refund.amount` (đã có).

### Shipping

**Bẫy S-1: Cancel khi shipment đã IN_TRANSIT**

Compensation đến muộn. Lúc này:
- Không thể "ungửi" hàng
- Phải đánh dấu CANCELLED + tạo "return order"
- Hoặc reject compensation và alert ops

**Bẫy S-2: Carrier API down**

Carrier API là external dependency → fail rate cao hơn DB internal. Cần:
- Timeout ngắn
- Circuit breaker (Resilience4j)
- Fallback: tạo shipment DRAFT, retry job sẽ book carrier sau

---

## 5. Test thủ công sau khi xong 4 services

### Setup

```bash
# Seed data: tạo products + initial inventory
docker exec -it postgres psql -U admin -d inventory_db
INSERT INTO products(id, name) VALUES 
  ('product-A', 'Product A'), 
  ('product-B', 'Product B');
INSERT INTO inventory(product_id, available_quantity, reserved_quantity) VALUES
  ('product-A', 100, 0),
  ('product-B', 50, 0);
```

### Test 1: Reserve stock thủ công (giả lập Saga gọi)

```bash
# Tạo ReserveStockCommand giả
COMMAND_ID=$(uuidgen)
ORDER_ID=$(uuidgen)
SAGA_ID=$(uuidgen)

docker exec -i kafka kafka-console-producer \
  --bootstrap-server kafka:29092 \
  --topic saga.commands.inventory <<EOF
{"commandId":"$COMMAND_ID","sagaId":"$SAGA_ID","orderId":"$ORDER_ID","commandType":"ReserveStock","items":[{"productId":"product-A","quantity":3}]}
EOF
```

Verify:
```sql
-- inventory_db
SELECT * FROM stock_reservations;        -- 1 row, status=RESERVED
SELECT * FROM inventory;                 -- product-A: available=97, reserved=3
SELECT * FROM inbox_events;              -- 1 row với command_id
SELECT * FROM outbox_events;             -- 1 row reply, processed_at sau 1s
```

### Test 2: Gửi cùng command 2 lần → idempotency

Gửi lại cùng `COMMAND_ID` → quan sát:
- Inbox không tăng (UNIQUE)
- Reservation không tăng (do early-return)
- Reply vẫn được gửi (hoặc skip tùy implementation)

### Test 3: Reserve hết stock → fail

Reserve 200 cho product-A (chỉ còn 97) → reply.success=false, reason="Insufficient stock".

### Test 4: Release (compensation)

Gửi `ReleaseStockCommand` → inventory.available trở về 100, reserved=0.

### Test 5: Payment with random failure

Set `payment.gateway.failure-rate: 0.5` → gọi charge 10 lần → ~5 lần fail.

### Test 6: Refund

Charge thành công → gọi refund → status=REFUNDED, refund record tạo.

---

## 6. Gợi ý thực hành

1. Code đầy đủ 3 service theo bài này.
2. Mỗi service test **độc lập** trước khi tích hợp (chưa cần Saga Orchestrator).
3. Stress test Inventory: 100 thread cùng reserve product-A 1 đơn vị, available=100 → confirm:
   - Đúng 100 reservation thành công
   - `available` về 0
   - Không có lỗi optimistic lock kéo dài (retry handle)
4. Test Payment với failure rate 30% → confirm:
   - Payment fail không leave dangling state
   - Outbox vẫn có reply (dù success=false)
5. Test Shipping cancel: tạo shipment → cancel → confirm status CANCELLED, carrier API được gọi (mock log).
6. Thử kill service giữa transaction (Inventory đang reserve) → confirm:
   - Inventory rollback (available không bị trừ)
   - Reservation không tạo
   - Inbox không có (rollback luôn)
   - Khi restart, command sẽ được redeliver từ Kafka → retry

---

## Checklist tự đánh giá

### Tổng quát
- [ ] Hiểu vai trò forward + compensation của mỗi service
- [ ] Mọi command handler đều có 3 tầng idempotency (Inbox + State + DB constraint)
- [ ] Mọi reply đi qua Outbox, không gọi kafkaTemplate trực tiếp

### Inventory
- [ ] Hiểu Reservation Pattern, vẽ được state diagram
- [ ] Phân biệt commit vs release reservation
- [ ] Xử lý concurrent reserve với optimistic lock + retry
- [ ] Có cleanup job cho expired reservation
- [ ] UNIQUE(order_id, product_id) đảm bảo natural idempotency

### Payment
- [ ] Generate idempotency-key cho gateway call
- [ ] Phân biệt status: PENDING / COMPLETED / FAILED / REFUNDED
- [ ] Refund chỉ áp dụng cho COMPLETED, idempotent
- [ ] Hiểu PCI-DSS và token (không lưu raw card)
- [ ] Mock gateway có failure rate configurable

### Shipping
- [ ] Hiểu vì sao compensation khó (recall vật lý)
- [ ] Cancel idempotent, không fail nếu shipment đã cancelled
- [ ] Carrier API có circuit breaker (production), timeout

---

**Bài tiếp theo:** `10 - Saga Orchestrator + State Machine` — code phần trung tâm điều phối toàn flow. Đây là bài "ghép tất cả lại" — sau bài 10 hệ thống chạy được end-to-end.
