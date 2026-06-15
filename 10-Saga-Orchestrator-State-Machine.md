# Saga Orchestrator + State Machine

> Bài 10 trong lộ trình Transaction Microservice nâng cao.
> Mục tiêu: Code service **Saga Orchestrator** — bộ não trung tâm điều phối toàn flow `Order → Inventory → Payment → Shipping`. Sau bài này, hệ thống chạy được **end-to-end happy path** và **3 failure scenarios**.

---

## 0. Vai trò Saga Orchestrator

Trong Orchestration pattern (đã chọn từ bài 7), Orchestrator là:

```
                  ┌──────────────────────────────┐
                  │   SAGA ORCHESTRATOR          │
                  │   - State machine            │
                  │   - Command dispatcher       │
                  │   - Reply handler            │
                  │   - Timeout watcher          │
                  │   - Saga persistence         │
                  └──────────────────────────────┘
                              │
        ┌────────────┬────────┼────────┬───────────┐
        │ commands   │ commands │ commands │ commands
        ▼            ▼          ▼          ▼
   Inventory     Payment    Shipping    Order
    Service     Service    Service    Service
        │            │          │          │
        └────────────┴── replies ──────────┘
                              │
                              ▼
                       Orchestrator
```

**Trách nhiệm cụ thể:**
1. **Khởi tạo Saga** khi nhận `OrderCreatedEvent` từ Order Service
2. **Persist Saga state** ở DB (saga_db) — recover được sau crash
3. **Gửi command** tới các service qua Outbox (Inventory → Payment → Shipping → Order confirm)
4. **Nhận reply**, advance state machine
5. **Trigger compensation** khi có service báo fail
6. **Detect timeout** — service không reply quá lâu
7. **Audit log** mọi step để debug và monitoring

---

## 1. State Machine — Sơ đồ đầy đủ

```
                        OrderCreatedEvent
                              │
                              ▼
                        ┌─────────────┐
                        │   STARTED   │
                        └──────┬──────┘
                               │ send ReserveStockCommand
                               ▼
                  ┌──────────────────────────┐
                  │ AWAITING_STOCK_RESERVATION│
                  └──────┬───────────────────┘
                         │
              ┌──────────┴──────────┐
              │                     │
       Stock OK│              Stock FAIL│
              ▼                     ▼
   ┌──────────────────┐    ┌────────────────────────┐
   │AWAITING_PAYMENT  │    │AWAITING_ORDER_CANCEL   │
   └─────┬────────────┘    │  (no compensation     │
         │                 │   needed - skip stock) │
   send ChargeCommand      └──────┬─────────────────┘
         │                        │
   ┌─────┴─────┐                  │
   │           │                  │
Pay OK│      Pay FAIL│            │
   ▼           ▼                  │
┌──────────┐ ┌──────────────────┐ │
│AWAITING_ │ │AWAITING_STOCK_   │ │
│SHIPMENT  │ │RELEASE           │ │
└────┬─────┘ └────┬─────────────┘ │
     │            │               │
     │            ▼               │
     │       ┌──────────────────┐ │
     │       │AWAITING_ORDER_   │◄┘
     │       │CANCELLATION      │
   send      └────┬─────────────┘
   Ship          │
   Command    send CancelOrder
     │            │
┌────┴──────┐     ▼
│           │  ┌─────────┐
Ship OK│  Ship FAIL│  │ FAILED  │  (terminal)
▼           ▼    └─────────┘
┌──────────┐  ┌────────────────────┐
│AWAITING_ │  │AWAITING_PAYMENT_   │
│ORDER_CONF│  │REFUND              │
└────┬─────┘  └────┬───────────────┘
     │             │
   send            ▼
   ConfirmOrder ┌────────────────────┐
     │          │AWAITING_STOCK_     │
     ▼          │RELEASE             │
┌──────────┐   └────┬───────────────┘
│COMPLETED │        │
└──────────┘        ▼
 (terminal)    ┌────────────────────┐
               │AWAITING_ORDER_     │
               │CANCELLATION        │
               └────┬───────────────┘
                    │
                    ▼
                ┌─────────┐
                │ FAILED  │
                └─────────┘
```

### Enum SagaState

```java
public enum SagaState {
    // Forward states
    STARTED,
    AWAITING_STOCK_RESERVATION,
    AWAITING_PAYMENT,
    AWAITING_SHIPMENT,
    AWAITING_ORDER_CONFIRMATION,
    
    // Compensation states
    AWAITING_PAYMENT_REFUND,
    AWAITING_STOCK_RELEASE,
    AWAITING_ORDER_CANCELLATION,
    
    // Terminal states
    COMPLETED,
    FAILED;

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED;
    }

    public boolean isCompensating() {
        return this == AWAITING_PAYMENT_REFUND 
            || this == AWAITING_STOCK_RELEASE
            || this == AWAITING_ORDER_CANCELLATION;
    }
}
```

### Bảng transition (lookup table)

| Current State | Event | Next State | Action |
|---|---|---|---|
| - | OrderCreated | STARTED | persist saga, send ReserveStock |
| STARTED | — | AWAITING_STOCK_RESERVATION | (auto) |
| AWAITING_STOCK_RESERVATION | StockReserved | AWAITING_PAYMENT | send ChargePayment |
| AWAITING_STOCK_RESERVATION | StockReserveFailed | AWAITING_ORDER_CANCELLATION | send CancelOrder |
| AWAITING_PAYMENT | PaymentCompleted | AWAITING_SHIPMENT | send CreateShipment |
| AWAITING_PAYMENT | PaymentFailed | AWAITING_STOCK_RELEASE | send ReleaseStock |
| AWAITING_SHIPMENT | ShipmentCreated | AWAITING_ORDER_CONFIRMATION | send ConfirmOrder |
| AWAITING_SHIPMENT | ShipmentFailed | AWAITING_PAYMENT_REFUND | send RefundPayment |
| AWAITING_ORDER_CONFIRMATION | OrderConfirmed | COMPLETED | (terminal) |
| AWAITING_PAYMENT_REFUND | PaymentRefunded | AWAITING_STOCK_RELEASE | send ReleaseStock |
| AWAITING_STOCK_RELEASE | StockReleased | AWAITING_ORDER_CANCELLATION | send CancelOrder |
| AWAITING_ORDER_CANCELLATION | OrderCancelled | FAILED | (terminal) |

→ **Đây là design tài liệu quan trọng nhất của bài.** Phải vẽ ra giấy hoặc Miro trước khi code.

---

## 2. Schema saga_db

`saga-orchestrator/.../V002__create_saga_tables.sql`:
```sql
CREATE TABLE saga_instances (
    id              UUID PRIMARY KEY,
    saga_type       VARCHAR(100) NOT NULL,              -- "OrderSaga"
    order_id        UUID NOT NULL UNIQUE,               -- 1 order = 1 saga
    customer_id     VARCHAR(100) NOT NULL,
    current_state   VARCHAR(100) NOT NULL,
    payload         JSONB NOT NULL,                     -- context: items, amount, address...
    failure_reason  TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    version         INT NOT NULL DEFAULT 0              -- optimistic lock
);

CREATE INDEX idx_saga_state ON saga_instances(current_state) 
    WHERE current_state NOT IN ('COMPLETED', 'FAILED');
CREATE INDEX idx_saga_updated_at ON saga_instances(updated_at);

-- Step audit log (mỗi command/reply là 1 step)
CREATE TABLE saga_steps (
    id              UUID PRIMARY KEY,
    saga_id         UUID NOT NULL REFERENCES saga_instances(id),
    step_no         INT NOT NULL,
    step_type       VARCHAR(50) NOT NULL,               -- COMMAND_SENT, REPLY_RECEIVED, TIMEOUT
    command_type    VARCHAR(100),
    target_service  VARCHAR(100),
    success         BOOLEAN,
    failure_reason  TEXT,
    payload         JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    
    UNIQUE (saga_id, step_no)
);

CREATE INDEX idx_saga_steps_saga_id ON saga_steps(saga_id);

-- Timeout tracking (chờ reply quá lâu)
CREATE TABLE saga_timeouts (
    saga_id         UUID PRIMARY KEY REFERENCES saga_instances(id),
    expected_state  VARCHAR(100) NOT NULL,
    deadline        TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_saga_timeouts_deadline ON saga_timeouts(deadline);
```

---

## 3. Domain Entity

`com/demo/saga/domain/SagaInstance.java`:
```java
@Entity
@Table(name = "saga_instances")
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SagaInstance {

    @Id
    private UUID id;

    @Column(name = "saga_type", nullable = false)
    private String sagaType;

    @Column(name = "order_id", nullable = false, unique = true)
    private UUID orderId;

    @Column(name = "customer_id", nullable = false)
    private String customerId;

    @Enumerated(EnumType.STRING)
    @Setter
    @Column(name = "current_state", nullable = false, length = 100)
    private SagaState currentState;

    @Type(JsonType.class)
    @Column(name = "payload", columnDefinition = "jsonb", nullable = false)
    private SagaPayload payload;

    @Setter
    @Column(name = "failure_reason")
    private String failureReason;

    @Column(name = "started_at", nullable = false)
    private Instant startedAt;

    @Setter
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Setter
    @Column(name = "completed_at")
    private Instant completedAt;

    @Version
    private Integer version;

    /**
     * Transition state với validation.
     */
    public void transitionTo(SagaState newState) {
        if (!isValidTransition(this.currentState, newState)) {
            throw new InvalidSagaTransitionException(
                "Invalid transition from " + this.currentState + " to " + newState);
        }
        log.info("Saga {} transition: {} → {}", id, currentState, newState);
        this.currentState = newState;
        this.updatedAt = Instant.now();

        if (newState.isTerminal()) {
            this.completedAt = Instant.now();
        }
    }

    public void markFailed(String reason) {
        this.failureReason = reason;
        this.currentState = SagaState.FAILED;
        this.completedAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    private boolean isValidTransition(SagaState from, SagaState to) {
        // Hard-coded transitions table; production có thể đọc từ config
        return switch (from) {
            case STARTED -> to == SagaState.AWAITING_STOCK_RESERVATION;
            case AWAITING_STOCK_RESERVATION -> 
                to == SagaState.AWAITING_PAYMENT 
                || to == SagaState.AWAITING_ORDER_CANCELLATION;
            case AWAITING_PAYMENT -> 
                to == SagaState.AWAITING_SHIPMENT 
                || to == SagaState.AWAITING_STOCK_RELEASE;
            case AWAITING_SHIPMENT -> 
                to == SagaState.AWAITING_ORDER_CONFIRMATION 
                || to == SagaState.AWAITING_PAYMENT_REFUND;
            case AWAITING_ORDER_CONFIRMATION -> to == SagaState.COMPLETED;
            case AWAITING_PAYMENT_REFUND -> to == SagaState.AWAITING_STOCK_RELEASE;
            case AWAITING_STOCK_RELEASE -> to == SagaState.AWAITING_ORDER_CANCELLATION;
            case AWAITING_ORDER_CANCELLATION -> to == SagaState.FAILED;
            case COMPLETED, FAILED -> false;  // terminal
        };
    }
}
```

`com/demo/saga/domain/SagaPayload.java`:
```java
@Data @NoArgsConstructor @AllArgsConstructor @Builder
public class SagaPayload {
    private List<OrderItemInfo> items;
    private BigDecimal totalAmount;
    private ShippingAddress shippingAddress;
    private String paymentMethod;
}
```

---

## 4. Saga Orchestrator — Core service

`com/demo/saga/application/SagaOrchestrator.java`:
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class SagaOrchestrator {

    private final SagaInstanceRepository sagaRepository;
    private final SagaStepRepository stepRepository;
    private final SagaTimeoutRepository timeoutRepository;
    private final CommandDispatcher dispatcher;
    private final IdempotentConsumer idempotentConsumer;

    private static final String CONSUMER_GROUP = "saga-orchestrator";

    // Timeout cho mỗi step (chờ reply tối đa)
    private static final Duration STEP_TIMEOUT = Duration.ofMinutes(2);

    /**
     * Bắt đầu Saga khi nhận OrderCreatedEvent từ Order Service.
     */
    @Transactional
    public void startSaga(OrderCreatedEvent event) {
        boolean processed = idempotentConsumer.processOnce(
            event.getEventId(),
            CONSUMER_GROUP,
            () -> doStartSaga(event)
        );
        
        if (!processed) {
            log.info("OrderCreatedEvent {} already processed, skipping", event.getEventId());
        }
    }

    private void doStartSaga(OrderCreatedEvent event) {
        UUID orderId = UUID.fromString(event.getAggregateId());
        
        // Idempotency phụ: 1 order chỉ 1 saga
        if (sagaRepository.existsByOrderId(orderId)) {
            log.info("Saga already exists for order {}, skipping", orderId);
            return;
        }

        UUID sagaId = UUID.randomUUID();
        SagaInstance saga = SagaInstance.builder()
            .id(sagaId)
            .sagaType("OrderSaga")
            .orderId(orderId)
            .customerId(event.getCustomerId())
            .currentState(SagaState.STARTED)
            .payload(SagaPayload.builder()
                .items(event.getItems())
                .totalAmount(event.getTotalAmount())
                // shippingAddress, paymentMethod: lấy từ event hoặc query Order Service
                .build())
            .startedAt(Instant.now())
            .updatedAt(Instant.now())
            .build();
        
        sagaRepository.save(saga);
        log.info("Saga {} created for order {}", sagaId, orderId);

        // Chuyển state và gửi command đầu tiên
        advanceTo(saga, SagaState.AWAITING_STOCK_RESERVATION);
    }

    /**
     * Xử lý reply từ các service.
     * Đây là trái tim của state machine.
     */
    @Transactional
    public void handleReply(SagaReply reply) {
        boolean processed = idempotentConsumer.processOnce(
            reply.getReplyId(),
            CONSUMER_GROUP,
            () -> doHandleReply(reply)
        );
        
        if (!processed) {
            log.info("Reply {} already processed", reply.getReplyId());
        }
    }

    private void doHandleReply(SagaReply reply) {
        UUID sagaId = UUID.fromString(reply.getSagaId());
        
        SagaInstance saga = sagaRepository.findById(sagaId)
            .orElseThrow(() -> new SagaNotFoundException(sagaId));

        if (saga.getCurrentState().isTerminal()) {
            log.warn("Saga {} is in terminal state {}, ignoring reply",
                sagaId, saga.getCurrentState());
            return;
        }

        log.info("Saga {} received reply: success={}, currentState={}",
            sagaId, reply.isSuccess(), saga.getCurrentState());

        // Audit log
        recordStep(saga, "REPLY_RECEIVED", reply.getClass().getSimpleName(),
            reply.isSuccess(), reply.getFailureReason());

        // Clear timeout (reply đã đến)
        timeoutRepository.deleteById(sagaId);

        // Determine next state based on current + reply
        SagaState nextState = determineNextState(
            saga.getCurrentState(), reply.isSuccess());
        
        if (nextState == SagaState.FAILED && !saga.getCurrentState().isCompensating()) {
            // Service đầu tiên fail → đánh dấu reason + start compensation
            saga.setFailureReason(reply.getFailureReason());
        }

        advanceTo(saga, nextState);
    }

    /**
     * Quyết định state tiếp theo dựa trên state hiện tại + kết quả reply.
     */
    private SagaState determineNextState(SagaState current, boolean success) {
        return switch (current) {
            case AWAITING_STOCK_RESERVATION -> 
                success ? SagaState.AWAITING_PAYMENT 
                        : SagaState.AWAITING_ORDER_CANCELLATION;
            
            case AWAITING_PAYMENT -> 
                success ? SagaState.AWAITING_SHIPMENT 
                        : SagaState.AWAITING_STOCK_RELEASE;
            
            case AWAITING_SHIPMENT -> 
                success ? SagaState.AWAITING_ORDER_CONFIRMATION 
                        : SagaState.AWAITING_PAYMENT_REFUND;
            
            case AWAITING_ORDER_CONFIRMATION -> 
                success ? SagaState.COMPLETED 
                        : SagaState.FAILED;     // edge: order confirm fail = critical
            
            // Compensation path
            case AWAITING_PAYMENT_REFUND -> 
                SagaState.AWAITING_STOCK_RELEASE;  // dù refund fail vẫn cố release stock
            
            case AWAITING_STOCK_RELEASE -> 
                SagaState.AWAITING_ORDER_CANCELLATION;
            
            case AWAITING_ORDER_CANCELLATION -> 
                SagaState.FAILED;
            
            default -> throw new IllegalStateException(
                "Cannot determine next state from " + current);
        };
    }

    /**
     * Advance saga to new state và trigger command tiếp theo.
     * Đây là hàm "chuyển trạng thái + side effect".
     */
    private void advanceTo(SagaInstance saga, SagaState newState) {
        saga.transitionTo(newState);
        sagaRepository.save(saga);

        // Terminal state → không gửi command nữa
        if (newState.isTerminal()) {
            log.info("Saga {} reached terminal state {}", saga.getId(), newState);
            return;
        }

        // Dispatch command tương ứng với state mới
        dispatchCommandForState(saga);
        
        // Setup timeout
        scheduleTimeout(saga);
    }

    private void dispatchCommandForState(SagaInstance saga) {
        SagaState state = saga.getCurrentState();
        UUID sagaId = saga.getId();
        UUID orderId = saga.getOrderId();
        SagaPayload payload = saga.getPayload();

        switch (state) {
            case AWAITING_STOCK_RESERVATION -> dispatcher.sendReserveStock(
                sagaId, orderId, payload.getItems());

            case AWAITING_PAYMENT -> dispatcher.sendChargePayment(
                sagaId, orderId, saga.getCustomerId(), payload.getTotalAmount());

            case AWAITING_SHIPMENT -> dispatcher.sendCreateShipment(
                sagaId, orderId, saga.getCustomerId(), payload.getShippingAddress());

            case AWAITING_ORDER_CONFIRMATION -> dispatcher.sendConfirmOrder(
                sagaId, orderId);

            // Compensation commands
            case AWAITING_PAYMENT_REFUND -> dispatcher.sendRefundPayment(
                sagaId, orderId, "Saga compensation: " + saga.getFailureReason());

            case AWAITING_STOCK_RELEASE -> dispatcher.sendReleaseStock(
                sagaId, orderId, payload.getItems());

            case AWAITING_ORDER_CANCELLATION -> dispatcher.sendCancelOrder(
                sagaId, orderId, saga.getFailureReason());

            default -> throw new IllegalStateException(
                "No command for state " + state);
        }

        recordStep(saga, "COMMAND_SENT", state.name(), null, null);
    }

    private void scheduleTimeout(SagaInstance saga) {
        SagaTimeout timeout = SagaTimeout.builder()
            .sagaId(saga.getId())
            .expectedState(saga.getCurrentState().name())
            .deadline(Instant.now().plus(STEP_TIMEOUT))
            .build();
        timeoutRepository.save(timeout);
    }

    private void recordStep(SagaInstance saga, String stepType, String detail,
                             Boolean success, String reason) {
        int nextStepNo = stepRepository.findMaxStepNo(saga.getId()) + 1;
        
        SagaStep step = SagaStep.builder()
            .id(UUID.randomUUID())
            .sagaId(saga.getId())
            .stepNo(nextStepNo)
            .stepType(stepType)
            .commandType(detail)
            .success(success)
            .failureReason(reason)
            .createdAt(Instant.now())
            .build();
        stepRepository.save(step);
    }
}
```

---

## 5. Command Dispatcher

`com/demo/saga/application/CommandDispatcher.java`:
```java
@Service
@RequiredArgsConstructor
public class CommandDispatcher {

    private final OutboxPublisher outboxPublisher;

    public void sendReserveStock(UUID sagaId, UUID orderId, List<OrderItemInfo> items) {
        ReserveStockCommand cmd = ReserveStockCommand.builder()
            .commandId(UUID.randomUUID())
            .sagaId(sagaId.toString())
            .orderId(orderId.toString())
            .items(items)
            .build();
        outboxPublisher.publishCommand(cmd, "saga.commands.inventory", sagaId.toString());
    }

    public void sendReleaseStock(UUID sagaId, UUID orderId, List<OrderItemInfo> items) {
        ReleaseStockCommand cmd = ReleaseStockCommand.builder()
            .commandId(UUID.randomUUID())
            .sagaId(sagaId.toString())
            .orderId(orderId.toString())
            .items(items)
            .build();
        outboxPublisher.publishCommand(cmd, "saga.commands.inventory", sagaId.toString());
    }

    public void sendChargePayment(UUID sagaId, UUID orderId, String customerId,
                                   BigDecimal amount) {
        ChargePaymentCommand cmd = ChargePaymentCommand.builder()
            .commandId(UUID.randomUUID())
            .sagaId(sagaId.toString())
            .orderId(orderId.toString())
            .customerId(customerId)
            .amount(amount)
            .build();
        outboxPublisher.publishCommand(cmd, "saga.commands.payment", sagaId.toString());
    }

    public void sendRefundPayment(UUID sagaId, UUID orderId, String reason) {
        RefundPaymentCommand cmd = RefundPaymentCommand.builder()
            .commandId(UUID.randomUUID())
            .sagaId(sagaId.toString())
            .orderId(orderId.toString())
            .reason(reason)
            .build();
        outboxPublisher.publishCommand(cmd, "saga.commands.payment", sagaId.toString());
    }

    public void sendCreateShipment(UUID sagaId, UUID orderId, String customerId,
                                    ShippingAddress address) {
        CreateShipmentCommand cmd = CreateShipmentCommand.builder()
            .commandId(UUID.randomUUID())
            .sagaId(sagaId.toString())
            .orderId(orderId.toString())
            .customerId(customerId)
            .address(address)
            .build();
        outboxPublisher.publishCommand(cmd, "saga.commands.shipping", sagaId.toString());
    }

    public void sendConfirmOrder(UUID sagaId, UUID orderId) {
        ConfirmOrderCommand cmd = ConfirmOrderCommand.builder()
            .commandId(UUID.randomUUID())
            .sagaId(sagaId.toString())
            .orderId(orderId.toString())
            .build();
        outboxPublisher.publishCommand(cmd, "saga.commands.order", sagaId.toString());
    }

    public void sendCancelOrder(UUID sagaId, UUID orderId, String reason) {
        CancelOrderCommand cmd = CancelOrderCommand.builder()
            .commandId(UUID.randomUUID())
            .sagaId(sagaId.toString())
            .orderId(orderId.toString())
            .reason(reason)
            .build();
        outboxPublisher.publishCommand(cmd, "saga.commands.order", sagaId.toString());
    }
}
```

→ **Lưu ý:** mọi command đi qua **Outbox** (đã handle Dual Write từ bài 5). `partitionKey = sagaId` để đảm bảo cùng saga vào cùng partition Kafka → order-preserved.

---

## 6. Kafka Listeners

### 6.1. Listen OrderCreatedEvent

```java
@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventListener {

    private final SagaOrchestrator orchestrator;
    private final ObjectMapper objectMapper;

    @KafkaListener(
        topics = "domain.events.order",
        groupId = "saga-orchestrator",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderEvent(ConsumerRecord<String, String> record, 
                              Acknowledgment ack) {
        String eventType = extractHeader(record, "event-type");

        try {
            if ("OrderCreated".equals(eventType)) {
                OrderCreatedEvent event = objectMapper.readValue(
                    record.value(), OrderCreatedEvent.class);
                orchestrator.startSaga(event);
            } else {
                log.debug("Ignoring event type: {}", eventType);
            }
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process order event", e);
            throw new RuntimeException("Order event processing failed", e);
        }
    }

    private String extractHeader(ConsumerRecord<String, String> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? "" : new String(header.value(), StandardCharsets.UTF_8);
    }
}
```

### 6.2. Listen Saga Replies

```java
@Component
@RequiredArgsConstructor
@Slf4j
public class SagaReplyListener {

    private final SagaOrchestrator orchestrator;
    private final ObjectMapper objectMapper;

    @KafkaListener(
        topics = "saga.replies",
        groupId = "saga-orchestrator",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void onReply(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            // Type discriminator from header
            String replyType = extractHeader(record, "event-type");
            SagaReply reply = deserializeReply(replyType, record.value());
            
            orchestrator.handleReply(reply);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process saga reply", e);
            throw new RuntimeException("Reply processing failed", e);
        }
    }

    private SagaReply deserializeReply(String type, String json) throws Exception {
        return switch (type) {
            case "InventoryCommandReply" -> 
                objectMapper.readValue(json, InventoryCommandReply.class);
            case "PaymentCommandReply" -> 
                objectMapper.readValue(json, PaymentCommandReply.class);
            case "ShippingCommandReply" -> 
                objectMapper.readValue(json, ShippingCommandReply.class);
            case "OrderCommandReply" -> 
                objectMapper.readValue(json, OrderCommandReply.class);
            default -> throw new IllegalArgumentException("Unknown reply type: " + type);
        };
    }

    private String extractHeader(ConsumerRecord<String, String> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? "" : new String(header.value(), StandardCharsets.UTF_8);
    }
}
```

→ **Mọi reply về cùng 1 topic `saga.replies`** → đơn giản hóa subscription. Phân biệt qua header `event-type`.

---

## 7. Timeout Handling

`com/demo/saga/application/SagaTimeoutWatcher.java`:
```java
@Component
@RequiredArgsConstructor
@Slf4j
public class SagaTimeoutWatcher {

    private final SagaTimeoutRepository timeoutRepository;
    private final SagaInstanceRepository sagaRepository;
    private final SagaOrchestrator orchestrator;

    /**
     * Mỗi 30s, check sagas đã quá deadline mà chưa nhận reply.
     */
    @Scheduled(fixedDelay = 30 * 1000)
    @Transactional
    public void checkTimeouts() {
        List<SagaTimeout> expired = timeoutRepository
            .findExpired(Instant.now());

        if (expired.isEmpty()) return;

        log.warn("Found {} expired saga timeouts", expired.size());

        for (SagaTimeout timeout : expired) {
            try {
                handleTimeout(timeout);
            } catch (Exception e) {
                log.error("Failed to handle timeout for saga {}", timeout.getSagaId(), e);
            }
        }
    }

    private void handleTimeout(SagaTimeout timeout) {
        SagaInstance saga = sagaRepository.findById(timeout.getSagaId())
            .orElse(null);
        
        if (saga == null) {
            // Saga đã bị xóa? Cleanup timeout record
            timeoutRepository.delete(timeout);
            return;
        }

        // Saga đã chuyển state → timeout không còn relevant
        if (!saga.getCurrentState().name().equals(timeout.getExpectedState())) {
            timeoutRepository.delete(timeout);
            return;
        }

        // Đã terminal
        if (saga.getCurrentState().isTerminal()) {
            timeoutRepository.delete(timeout);
            return;
        }

        log.error("Saga {} TIMEOUT in state {} after deadline", 
            saga.getId(), saga.getCurrentState());

        // Strategy 1: Retry — resend command
        // Strategy 2: Treat as failure — trigger compensation
        // Demo: Strategy 2 cho đơn giản
        
        // Generate synthetic failure reply
        SagaReply syntheticReply = createTimeoutReply(saga);
        orchestrator.handleReply(syntheticReply);
        
        timeoutRepository.delete(timeout);
    }

    private SagaReply createTimeoutReply(SagaInstance saga) {
        // Tạo reply giả với success=false để trigger compensation
        return GenericSagaReply.builder()
            .replyId(UUID.randomUUID())
            .commandId(UUID.randomUUID())
            .sagaId(saga.getId().toString())
            .success(false)
            .failureReason("Timeout waiting for " + saga.getCurrentState())
            .build();
    }
}
```

→ **Production cân nhắc:** Retry với exponential backoff trước khi fail. Demo chọn đơn giản: timeout = fail.

---

## 8. Recovery sau crash

Orchestrator có thể crash giữa flow. State đã persist trong DB → restart thì:

1. **Saga đang ở state X** → đợi reply
2. Reply có thể đã được send và lưu trong Kafka → Orchestrator đọc lại từ offset cũ
3. **Hoặc** reply chưa được send → service sẽ resend nếu chưa ack (vì service vẫn giữ command trong inbox)
4. **Hoặc** timeout watcher sẽ trigger fail

→ **Crash recovery tự động** nhờ:
- State persisted trong DB (saga_instances)
- Reply lưu trong Kafka log (consumer offset based)
- Idempotency (Inbox) — replay không gây side effect kép
- Timeout watcher — fallback cho mọi trường hợp

### Recovery scenario diagram

```
T0: Saga ở AWAITING_PAYMENT
T1: PaymentService completes, publish reply to Kafka
T2: Orchestrator CRASH (chưa kịp đọc reply)
T3: Orchestrator RESTART
T4: Kafka consumer resume từ last committed offset
    → Đọc lại reply, xử lý qua handleReply()
    → Idempotency check (inbox) — nếu đã process, skip
    → Nếu chưa, advance state → AWAITING_SHIPMENT
T5: Tiếp tục flow bình thường
```

→ Không cần code logic recovery đặc biệt. Kiến trúc đã tự khôi phục.

---

## 9. REST API cho monitoring

```java
@RestController
@RequestMapping("/api/sagas")
@RequiredArgsConstructor
public class SagaController {

    private final SagaInstanceRepository sagaRepository;
    private final SagaStepRepository stepRepository;

    @GetMapping("/{sagaId}")
    public SagaDetailResponse getSaga(@PathVariable UUID sagaId) {
        SagaInstance saga = sagaRepository.findById(sagaId)
            .orElseThrow();
        List<SagaStep> steps = stepRepository.findBySagaIdOrderByStepNo(sagaId);
        return new SagaDetailResponse(saga, steps);
    }

    @GetMapping("/by-order/{orderId}")
    public SagaDetailResponse getSagaByOrder(@PathVariable UUID orderId) {
        SagaInstance saga = sagaRepository.findByOrderId(orderId)
            .orElseThrow();
        List<SagaStep> steps = stepRepository.findBySagaIdOrderByStepNo(saga.getId());
        return new SagaDetailResponse(saga, steps);
    }

    @GetMapping("/active")
    public List<SagaSummary> getActiveSagas() {
        return sagaRepository.findActive().stream()
            .map(this::toSummary).toList();
    }

    @GetMapping("/stats")
    public SagaStats getStats() {
        return SagaStats.builder()
            .total(sagaRepository.count())
            .completed(sagaRepository.countByCurrentState(SagaState.COMPLETED))
            .failed(sagaRepository.countByCurrentState(SagaState.FAILED))
            .active(sagaRepository.countActive())
            .build();
    }
}
```

→ Có endpoint này để debug rất nhanh: `GET /api/sagas/by-order/{orderId}` show toàn bộ flow + steps.

---

## 10. Test end-to-end

### 10.1. Chuẩn bị

```bash
# Start tất cả
docker compose up -d
bash infra/kafka/create-topics.sh

mvn clean install -DskipTests
mvn spring-boot:run -pl order-service &
mvn spring-boot:run -pl inventory-service &
mvn spring-boot:run -pl payment-service &
mvn spring-boot:run -pl shipping-service &
mvn spring-boot:run -pl saga-orchestrator &

# Seed inventory
docker exec -it postgres psql -U admin -d inventory_db -c "
  INSERT INTO products(id, name) VALUES ('product-A', 'Product A');
  INSERT INTO inventory(product_id, available_quantity) VALUES ('product-A', 100);
"
```

### 10.2. Happy path

```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "customer-123",
    "items": [
      {"productId": "product-A", "quantity": 2, "unitPrice": 50.00}
    ]
  }'

# Response: 202 với orderId, status=PENDING

# Đợi 3-5 giây, query:
curl http://localhost:8081/api/orders/{orderId}
# Status: CONFIRMED

# Query saga:
curl http://localhost:8085/api/sagas/by-order/{orderId}
# Show full flow:
# STARTED → AWAITING_STOCK_RESERVATION → AWAITING_PAYMENT 
# → AWAITING_SHIPMENT → AWAITING_ORDER_CONFIRMATION → COMPLETED
```

### 10.3. Verify dataflow

```sql
-- saga_db
SELECT id, current_state, started_at, completed_at FROM saga_instances;
SELECT * FROM saga_steps WHERE saga_id = '...' ORDER BY step_no;
-- Show: command_sent, reply_received cho từng step

-- order_db
SELECT * FROM orders;          -- status=CONFIRMED
SELECT * FROM outbox_events;   -- all processed
SELECT * FROM inbox_events;    -- ConfirmOrderCommand đã process

-- inventory_db
SELECT * FROM inventory;       -- available=98 (đã commit, reserved=0)
SELECT * FROM stock_reservations;  -- status=COMMITTED

-- payment_db
SELECT * FROM payments;        -- status=COMPLETED

-- shipping_db
SELECT * FROM shipments;       -- status=CREATED, tracking_number có
```

### 10.4. Failure scenarios

**Scenario A: Stock insufficient**
```bash
# Order với quantity vượt inventory
curl -X POST http://localhost:8081/api/orders -d '{
  "customerId": "c1", 
  "items": [{"productId": "product-A", "quantity": 9999, "unitPrice": 50}]
}'

# Verify saga: STARTED → AWAITING_STOCK_RESERVATION → AWAITING_ORDER_CANCELLATION → FAILED
# Order status: CANCELLED
# Inventory: không thay đổi
```

**Scenario B: Payment fail**
```bash
# Tăng failure rate payment service:
# application.yml: payment.gateway.failure-rate: 1.0
# Restart payment-service

curl -X POST http://localhost:8081/api/orders -d '{...}'

# Verify saga flow:
# STARTED → AWAITING_STOCK_RESERVATION → AWAITING_PAYMENT 
# → AWAITING_STOCK_RELEASE → AWAITING_ORDER_CANCELLATION → FAILED
# Inventory: reserved về 0 (compensation OK)
# Order: CANCELLED
```

**Scenario C: Shipping fail**
```bash
# Hard-code shipping fail trong MockCarrierApi
curl -X POST http://localhost:8081/api/orders -d '{...}'

# Verify saga:
# ... → AWAITING_SHIPMENT → AWAITING_PAYMENT_REFUND 
# → AWAITING_STOCK_RELEASE → AWAITING_ORDER_CANCELLATION → FAILED
# Payment: REFUNDED
# Inventory: released
# Order: CANCELLED
```

---

## 11. Bẫy phổ biến

### Bẫy 1: Side effect trong `determineNextState`
Function này phải **PURE** — chỉ tính next state, không call DB, không send message. Side effect chỉ trong `advanceTo` và `dispatchCommandForState`.

### Bẫy 2: Quên xử lý timeout
Service down → reply không bao giờ đến → Saga treo vĩnh viễn. **Mọi command phải có deadline**.

### Bẫy 3: Compensation chain phải bền bỉ
Compensation cũng có thể fail. Strategy:
- Retry với backoff
- Nếu N lần fail → escalate (alert, manual intervention)
- **Không** rollback compensation (impossible)

### Bẫy 4: Race condition giữa reply và timeout
Reply đến cùng lúc timeout fires → cả 2 advance state → double transition.
→ Optimistic lock (`@Version` trên SagaInstance) bảo vệ — 1 trong 2 sẽ fail và retry.

### Bẫy 5: Lost reply
Reply publish nhưng listener crash trước commit offset → Kafka redeliver → Idempotent consumer handle. Đảm bảo Idempotency mọi nơi.

### Bẫy 6: Saga payload quá lớn
Saga payload trong DB là JSONB. Đừng nhét full product data, image URLs. Chỉ những gì cần để dispatch commands.

### Bẫy 7: Không có monitoring
Saga FAILED phải có alert. Production: dashboard hiển thị success rate, P99 duration, failure breakdown by state.

### Bẫy 8: Compensation skip nếu service đã down
Service down → command vào Kafka, không ai consume → timeout → Saga FAILED nhưng compensation chưa chạy thực sự.
→ Service recover → consume command (idempotent) → compensation chạy.
→ Eventual consistency. Đảm bảo compensation command **không expire** trong Kafka.

### Bẫy 9: God Orchestrator
Orchestrator chứa business logic của các service. **Sai.** Orchestrator chỉ:
- Quyết định next state
- Send command
- Receive reply

Logic business (stock reservation rules, payment validation) ở từng service.

### Bẫy 10: Hardcode timeout
Step timeout 2 phút cho tất cả. Sai. Payment có thể chậm hơn (gateway latency), Order confirm nhanh hơn. Configure per-step.

---

## 12. Gợi ý thực hành

1. Vẽ state machine ra giấy/Miro **trước khi code**. Sai 1 transition = bug khó tìm.
2. Code Orchestrator theo bài này. Test thủ công happy path trước.
3. Test 3 failure scenarios. Verify compensation đầy đủ trong mọi DB.
4. Kill `saga-orchestrator` giữa flow → restart → verify saga resume đúng (không bị stuck).
5. Stress test: 100 order đồng thời → verify không có saga nào FAILED do race condition.
6. Force timeout: stop `payment-service` → tạo order → quan sát:
   - Saga ở AWAITING_PAYMENT
   - Sau 2 phút → timeout watcher fire
   - Compensation chạy: release stock, cancel order
7. Restart payment-service → command vẫn trong Kafka → consume + xử lý (idempotent) → OK.

---

## Checklist tự đánh giá

- [ ] Vẽ được state machine đầy đủ (10 states + transitions) từ trí nhớ
- [ ] Hiểu vai trò mỗi component: Orchestrator, Dispatcher, ReplyListener, TimeoutWatcher
- [ ] `determineNextState` là pure function, side effect riêng
- [ ] Persist Saga state trong DB, sống sót crash
- [ ] Mọi command đi qua Outbox (không gọi Kafka trực tiếp)
- [ ] Mọi reply xử lý idempotent (Inbox)
- [ ] Timeout watcher backup khi service không reply
- [ ] Compensation tự động trigger khi forward fail
- [ ] Có API monitoring `/api/sagas/by-order/{orderId}`
- [ ] Test happy path + 3 failure scenarios đầy đủ
- [ ] Verify dataflow trên cả 5 database

---

**Bài tiếp theo:** `11 - End-to-End Testing với Testcontainers` — viết integration test tự động cho toàn bộ flow.
