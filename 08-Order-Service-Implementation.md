# Implement Order Service đầy đủ

> Bài 8 trong lộ trình Transaction Microservice nâng cao.
> Mục tiêu: Code Order Service end-to-end — REST API tạo order, lưu DB, publish event qua Outbox, consume command từ Saga Orchestrator, idempotent consumer. Đây là **service đầu tiên** chạy đúng với tất cả pattern đã học.

---

## 0. Order Service trong Saga — Responsibility

Order Service là **entry point** của flow:

```
Client → POST /api/orders → OrderService
                                ↓
                       (1) INSERT order (status=PENDING)
                       (2) Publish OrderCreated event (qua Outbox)
                                ↓
                       Saga Orchestrator nhận event → start Saga
                                ↓
                       Saga điều phối: Inventory → Payment → Shipping
                                ↓
                       Khi xong: Saga gửi ConfirmOrder hoặc CancelOrder command
                                ↓
                       OrderService consume command → update order status
```

**Trách nhiệm cụ thể:**
1. Cung cấp REST API: tạo order, query order
2. Lưu Order vào local DB với optimistic locking
3. Publish `OrderCreatedEvent` qua Outbox (atomic với INSERT order)
4. Consume command từ Saga (`ConfirmOrderCommand`, `CancelOrderCommand`)
5. Update order status idempotent (qua Inbox)

---

## 1. Folder structure của Order Service

```
order-service/
├── pom.xml
└── src/
    ├── main/
    │   ├── java/com/demo/order/
    │   │   ├── OrderServiceApplication.java
    │   │   │
    │   │   ├── api/                          # REST layer
    │   │   │   ├── OrderController.java
    │   │   │   ├── dto/
    │   │   │   │   ├── CreateOrderRequest.java
    │   │   │   │   ├── OrderResponse.java
    │   │   │   │   └── OrderItemDto.java
    │   │   │   └── mapper/
    │   │   │       └── OrderMapper.java
    │   │   │
    │   │   ├── domain/                       # Business core
    │   │   │   ├── Order.java                # Entity
    │   │   │   ├── OrderItem.java            # Entity
    │   │   │   ├── OrderStatus.java          # Enum
    │   │   │   └── OrderStatusTransition.java
    │   │   │
    │   │   ├── application/                  # Application services
    │   │   │   ├── OrderService.java         # Business logic
    │   │   │   └── SagaCommandHandler.java   # Handle commands from Saga
    │   │   │
    │   │   ├── infra/                        # Infrastructure
    │   │   │   ├── persistence/
    │   │   │   │   └── OrderRepository.java
    │   │   │   └── messaging/
    │   │   │       └── SagaCommandListener.java
    │   │   │
    │   │   ├── config/
    │   │   │   ├── KafkaConfig.java
    │   │   │   └── JacksonConfig.java
    │   │   │
    │   │   └── exception/
    │   │       ├── OrderNotFoundException.java
    │   │       ├── InvalidOrderStateException.java
    │   │       └── GlobalExceptionHandler.java
    │   │
    │   └── resources/
    │       ├── application.yml
    │       └── db/migration/
    │           ├── V001__create_messaging_tables.sql
    │           └── V002__create_order_tables.sql
    │
    └── test/
        └── java/com/demo/order/
            ├── OrderServiceIntegrationTest.java
            └── api/OrderControllerTest.java
```

---

## 2. POM dependencies

`order-service/pom.xml`:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.demo</groupId>
        <artifactId>saga-demo-parent</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>

    <artifactId>order-service</artifactId>

    <dependencies>
        <!-- Internal libs -->
        <dependency>
            <groupId>com.demo</groupId>
            <artifactId>common-lib</artifactId>
            <version>1.0.0-SNAPSHOT</version>
        </dependency>
        <dependency>
            <groupId>com.demo</groupId>
            <artifactId>messaging-lib</artifactId>
            <version>1.0.0-SNAPSHOT</version>
        </dependency>

        <!-- Spring Boot starters -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-jpa</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-validation</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>

        <!-- Database -->
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-core</artifactId>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-database-postgresql</artifactId>
        </dependency>
        <dependency>
            <groupId>com.vladmihalcea</groupId>
            <artifactId>hibernate-types-60</artifactId>
            <version>2.21.1</version>
        </dependency>

        <!-- Observability -->
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-tracing-bridge-otel</artifactId>
        </dependency>
        <dependency>
            <groupId>io.opentelemetry</groupId>
            <artifactId>opentelemetry-exporter-otlp</artifactId>
        </dependency>
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-registry-prometheus</artifactId>
        </dependency>

        <!-- Utilities -->
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.mapstruct</groupId>
            <artifactId>mapstruct</artifactId>
            <version>${mapstruct.version}</version>
        </dependency>

        <!-- Test -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>postgresql</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>kafka</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

---

## 3. Domain layer

### 3.1. OrderStatus enum + transitions

`com/demo/order/domain/OrderStatus.java`:
```java
public enum OrderStatus {
    PENDING,          // Vừa tạo, đang chờ Saga
    CONFIRMED,        // Saga thành công
    CANCELLED;        // Saga fail, đã rollback

    public boolean canTransitionTo(OrderStatus target) {
        return switch (this) {
            case PENDING -> target == CONFIRMED || target == CANCELLED;
            case CONFIRMED, CANCELLED -> false;  // terminal states
        };
    }

    public boolean isTerminal() {
        return this == CONFIRMED || this == CANCELLED;
    }
}
```

→ **State-based idempotency** built-in: nếu nhận command Confirm 2 lần, lần 2 sẽ thấy state đã CONFIRMED → reject transition.

### 3.2. Order entity

`com/demo/order/domain/Order.java`:
```java
@Entity
@Table(name = "orders")
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Order {

    @Id
    private UUID id;

    @Column(name = "customer_id", nullable = false)
    private String customerId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 50)
    private OrderStatus status;

    @Column(name = "total_amount", nullable = false, precision = 19, scale = 4)
    private BigDecimal totalAmount;

    @Column(name = "saga_id")
    private UUID sagaId;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Setter
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true,
               fetch = FetchType.EAGER)
    @Builder.Default
    private List<OrderItem> items = new ArrayList<>();

    @Version
    @Column(nullable = false)
    private Integer version;     // Optimistic lock

    // Factory method
    public static Order create(String customerId, List<OrderItem> items) {
        BigDecimal total = items.stream()
            .map(item -> item.getUnitPrice().multiply(BigDecimal.valueOf(item.getQuantity())))
            .reduce(BigDecimal.ZERO, BigDecimal::add);

        UUID orderId = UUID.randomUUID();
        Order order = Order.builder()
            .id(orderId)
            .customerId(customerId)
            .status(OrderStatus.PENDING)
            .totalAmount(total)
            .createdAt(Instant.now())
            .updatedAt(Instant.now())
            .version(0)
            .build();

        items.forEach(item -> {
            item.setId(UUID.randomUUID());
            item.setOrder(order);
            order.items.add(item);
        });

        return order;
    }

    // State transition methods (business logic ở đây, không phải ở Service)
    public void confirm(UUID sagaId) {
        if (!status.canTransitionTo(OrderStatus.CONFIRMED)) {
            throw new InvalidOrderStateException(
                "Cannot confirm order in status " + status);
        }
        this.status = OrderStatus.CONFIRMED;
        this.sagaId = sagaId;
        this.updatedAt = Instant.now();
    }

    public void cancel(UUID sagaId, String reason) {
        if (status == OrderStatus.CANCELLED) return;  // idempotent
        if (!status.canTransitionTo(OrderStatus.CANCELLED)) {
            throw new InvalidOrderStateException(
                "Cannot cancel order in status " + status);
        }
        this.status = OrderStatus.CANCELLED;
        this.sagaId = sagaId;
        this.updatedAt = Instant.now();
    }

    public void attachSaga(UUID sagaId) {
        this.sagaId = sagaId;
    }
}
```

**Lưu ý quan trọng:**
- **Domain logic ở Entity**, không phải ở Service. Đây là **Rich Domain Model** (DDD).
- `@Version` để optimistic lock — chống lost update khi 2 thread cùng update.
- Phương thức `confirm()`, `cancel()` tự validate state transition.

### 3.3. OrderItem entity

`com/demo/order/domain/OrderItem.java`:
```java
@Entity
@Table(name = "order_items")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderItem {
    @Id
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "order_id", nullable = false)
    private Order order;

    @Column(name = "product_id", nullable = false)
    private String productId;

    @Column(nullable = false)
    private Integer quantity;

    @Column(name = "unit_price", nullable = false, precision = 19, scale = 4)
    private BigDecimal unitPrice;
}
```

---

## 4. Repository

`com/demo/order/infra/persistence/OrderRepository.java`:
```java
public interface OrderRepository extends JpaRepository<Order, UUID> {

    Optional<Order> findBySagaId(UUID sagaId);

    @Query("SELECT o FROM Order o WHERE o.customerId = :customerId ORDER BY o.createdAt DESC")
    List<Order> findByCustomerIdOrderByCreatedAtDesc(@Param("customerId") String customerId);
}
```

---

## 5. DTO Layer

### 5.1. CreateOrderRequest

`com/demo/order/api/dto/CreateOrderRequest.java`:
```java
@Data
public class CreateOrderRequest {

    @NotBlank(message = "Customer ID is required")
    private String customerId;

    @NotEmpty(message = "Order must have at least 1 item")
    @Valid
    private List<OrderItemDto> items;
}
```

### 5.2. OrderItemDto

```java
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderItemDto {

    @NotBlank
    private String productId;

    @NotNull
    @Min(1)
    private Integer quantity;

    @NotNull
    @DecimalMin(value = "0.0", inclusive = false)
    private BigDecimal unitPrice;
}
```

### 5.3. OrderResponse

```java
@Data
@Builder
public class OrderResponse {
    private UUID id;
    private String customerId;
    private String status;
    private BigDecimal totalAmount;
    private UUID sagaId;
    private List<OrderItemDto> items;
    private Instant createdAt;
    private Instant updatedAt;
}
```

### 5.4. Mapper

`com/demo/order/api/mapper/OrderMapper.java`:
```java
@Mapper(componentModel = "spring")
public interface OrderMapper {

    OrderResponse toResponse(Order order);

    @Mapping(target = "productId", source = "productId")
    OrderItemDto toItemDto(OrderItem item);

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "order", ignore = true)
    OrderItem toItemEntity(OrderItemDto dto);

    List<OrderItem> toItemEntities(List<OrderItemDto> dtos);
}
```

---

## 6. Application Service — Trái tim của bài này

`com/demo/order/application/OrderService.java`:
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderMapper orderMapper;
    private final OutboxPublisher outboxPublisher;

    private static final String ORDER_EVENTS_TOPIC = "domain.events.order";

    /**
     * Tạo order. Đây là điểm bắt đầu của Saga.
     *
     * @Transactional ôm cả 2 việc:
     *   1. INSERT order vào DB
     *   2. INSERT outbox event
     * → Đảm bảo atomicity. ACID lo. Không có Dual Write.
     */
    @Transactional
    public OrderResponse createOrder(CreateOrderRequest request) {
        log.info("Creating order for customer={}, items={}",
            request.getCustomerId(), request.getItems().size());

        // 1. Tạo domain entity
        List<OrderItem> items = request.getItems().stream()
            .map(dto -> OrderItem.builder()
                .productId(dto.getProductId())
                .quantity(dto.getQuantity())
                .unitPrice(dto.getUnitPrice())
                .build())
            .toList();

        Order order = Order.create(request.getCustomerId(), items);

        // 2. Lưu DB
        orderRepository.save(order);
        log.info("Order saved: id={}, status={}, total={}",
            order.getId(), order.getStatus(), order.getTotalAmount());

        // 3. Publish event qua Outbox (CÙNG transaction!)
        OrderCreatedEvent event = OrderCreatedEvent.builder()
            .eventId(UUID.randomUUID())
            .aggregateId(order.getId().toString())
            .occurredAt(Instant.now())
            .version(1)
            .customerId(order.getCustomerId())
            .totalAmount(order.getTotalAmount())
            .items(request.getItems())
            .build();

        outboxPublisher.publish(event, ORDER_EVENTS_TOPIC);
        log.info("OrderCreatedEvent published to outbox: eventId={}, orderId={}",
            event.getEventId(), order.getId());

        return orderMapper.toResponse(order);
    }

    @Transactional(readOnly = true)
    public OrderResponse getOrder(UUID orderId) {
        Order order = orderRepository.findById(orderId)
            .orElseThrow(() -> new OrderNotFoundException(orderId));
        return orderMapper.toResponse(order);
    }

    @Transactional(readOnly = true)
    public List<OrderResponse> getOrdersByCustomer(String customerId) {
        return orderRepository.findByCustomerIdOrderByCreatedAtDesc(customerId)
            .stream()
            .map(orderMapper::toResponse)
            .toList();
    }
}
```

**Điểm chốt:**
- `@Transactional` bao **cả 2** INSERT (orders, outbox_events) → atomic
- Outbox Relay (đã có ở `messaging-lib`) sẽ tự pick lên và publish sang Kafka
- **Không** gọi `kafkaTemplate.send()` ở đây — đây là chính là cái sai mà ta tránh

---

## 7. Saga Command Handler

`com/demo/order/application/SagaCommandHandler.java`:
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class SagaCommandHandler {

    private final OrderRepository orderRepository;
    private final OutboxPublisher outboxPublisher;
    private final IdempotentConsumer idempotentConsumer;

    private static final String CONSUMER_GROUP = "order-service.saga-handler";
    private static final String SAGA_REPLIES_TOPIC = "saga.replies";

    /**
     * Xử lý ConfirmOrderCommand từ Saga Orchestrator.
     * 
     * @Transactional bao 3 việc:
     *   1. INSERT inbox (idempotency)
     *   2. UPDATE order status
     *   3. INSERT outbox (reply về Saga)
     */
    @Transactional
    public void handleConfirmOrder(ConfirmOrderCommand command) {
        log.info("Handling ConfirmOrderCommand: orderId={}, sagaId={}",
            command.getOrderId(), command.getSagaId());

        boolean processed = idempotentConsumer.processOnce(
            command.getCommandId(),
            CONSUMER_GROUP,
            () -> doConfirmOrder(command)
        );

        if (!processed) {
            log.info("Command {} already processed, skipped", command.getCommandId());
        }
    }

    private void doConfirmOrder(ConfirmOrderCommand command) {
        UUID orderId = UUID.fromString(command.getOrderId());
        UUID sagaId = UUID.fromString(command.getSagaId());

        Order order = orderRepository.findById(orderId)
            .orElseThrow(() -> new OrderNotFoundException(orderId));

        // State-based idempotency (thêm 1 lớp bảo vệ)
        if (order.getStatus() == OrderStatus.CONFIRMED) {
            log.info("Order {} already confirmed, skipping", orderId);
            sendReply(command, true, null);
            return;
        }

        order.confirm(sagaId);
        orderRepository.save(order);
        log.info("Order {} confirmed by saga {}", orderId, sagaId);

        sendReply(command, true, null);
    }

    @Transactional
    public void handleCancelOrder(CancelOrderCommand command) {
        log.info("Handling CancelOrderCommand: orderId={}, sagaId={}, reason={}",
            command.getOrderId(), command.getSagaId(), command.getReason());

        idempotentConsumer.processOnce(
            command.getCommandId(),
            CONSUMER_GROUP,
            () -> doCancelOrder(command)
        );
    }

    private void doCancelOrder(CancelOrderCommand command) {
        UUID orderId = UUID.fromString(command.getOrderId());
        UUID sagaId = UUID.fromString(command.getSagaId());

        Order order = orderRepository.findById(orderId)
            .orElseThrow(() -> new OrderNotFoundException(orderId));

        if (order.getStatus() == OrderStatus.CANCELLED) {
            log.info("Order {} already cancelled, skipping", orderId);
            sendReply(command, true, null);
            return;
        }

        order.cancel(sagaId, command.getReason());
        orderRepository.save(order);
        log.info("Order {} cancelled by saga {}", orderId, sagaId);

        sendReply(command, true, null);
    }

    private void sendReply(SagaCommand command, boolean success, String failureReason) {
        OrderCommandReply reply = OrderCommandReply.builder()
            .replyId(UUID.randomUUID())
            .commandId(command.getCommandId())
            .sagaId(command.getSagaId())
            .success(success)
            .failureReason(failureReason)
            .build();

        // Reply gửi qua Outbox để đảm bảo Saga chắc chắn nhận được
        outboxPublisher.publishCommand(reply, SAGA_REPLIES_TOPIC, command.getSagaId());
    }
}
```

**Lưu ý quan trọng:**
- **3 tầng idempotency**:
  1. **Inbox** (`idempotentConsumer.processOnce`) — chặn duplicate message
  2. **State check** (`if (order.getStatus() == CONFIRMED)`) — chặn business duplicate
  3. **DB UNIQUE constraint** trên inbox primary key — chặn race condition

- **Reply gửi qua Outbox** — nếu gửi trực tiếp qua KafkaTemplate sẽ lại có Dual Write.

→ Cần update `OutboxPublisher` ở `messaging-lib` có thêm method `publishCommand` (chỉ là wrapper khác cho `publish`, với event type là command/reply).

---

## 8. Kafka Listener — Cầu nối Kafka → Handler

`com/demo/order/infra/messaging/SagaCommandListener.java`:
```java
@Component
@RequiredArgsConstructor
@Slf4j
public class SagaCommandListener {

    private final SagaCommandHandler handler;
    private final ObjectMapper objectMapper;

    @KafkaListener(
        topics = "saga.commands.order",
        groupId = "order-service",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void onCommand(
        ConsumerRecord<String, String> record,
        Acknowledgment ack
    ) {
        String commandType = extractHeader(record, "event-type");
        String payload = record.value();

        log.info("Received command: type={}, partition={}, offset={}",
            commandType, record.partition(), record.offset());

        try {
            switch (commandType) {
                case "ConfirmOrder" -> {
                    ConfirmOrderCommand cmd = objectMapper.readValue(
                        payload, ConfirmOrderCommand.class);
                    handler.handleConfirmOrder(cmd);
                }
                case "CancelOrder" -> {
                    CancelOrderCommand cmd = objectMapper.readValue(
                        payload, CancelOrderCommand.class);
                    handler.handleCancelOrder(cmd);
                }
                default -> log.warn("Unknown command type: {}", commandType);
            }
            
            ack.acknowledge();  // Manual ack SAU khi xử lý thành công
            
        } catch (Exception e) {
            log.error("Failed to process command type={}", commandType, e);
            // Không ack → Kafka redeliver
            // Sau N lần fail → DLQ (config ở KafkaConfig)
            throw new RuntimeException("Command processing failed", e);
        }
    }

    private String extractHeader(ConsumerRecord<String, String> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? "" : new String(header.value(), StandardCharsets.UTF_8);
    }
}
```

---

## 9. Kafka Configuration

`com/demo/order/config/KafkaConfig.java`:
```java
@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // ============ PRODUCER ============
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, 3);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // ============ CONSUMER ============
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "order-service");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> 
            kafkaListenerContainerFactory(
                KafkaTemplate<String, String> kafkaTemplate) {
        
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // Error handling + DLQ
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
            new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> new TopicPartition("saga.dlq", record.partition())),
            new FixedBackOff(1000L, 3)  // retry 3 lần, mỗi lần cách 1s
        );
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}
```

**Các config trọng yếu (đã giải thích ở bài 6-7):**
- `enable.idempotence=true`: Kafka producer chống duplicate trong cùng session
- `acks=all`: đợi tất cả replica ack — đảm bảo durability
- `enable-auto-commit=false`: manual ack — at-least-once delivery
- `isolation.level=read_committed`: chỉ đọc message đã commit của transactional producer
- DLQ: sau 3 lần retry, message vào `saga.dlq` — cần human investigate

---

## 10. REST Controller

`com/demo/order/api/OrderController.java`:
```java
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
@Slf4j
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)        // 202: order tạo xong, Saga đang xử lý
    public OrderResponse createOrder(@Valid @RequestBody CreateOrderRequest request) {
        log.info("POST /api/orders, customer={}", request.getCustomerId());
        return orderService.createOrder(request);
    }

    @GetMapping("/{orderId}")
    public OrderResponse getOrder(@PathVariable UUID orderId) {
        return orderService.getOrder(orderId);
    }

    @GetMapping("/by-customer/{customerId}")
    public List<OrderResponse> getOrdersByCustomer(@PathVariable String customerId) {
        return orderService.getOrdersByCustomer(customerId);
    }
}
```

**Quan trọng**: trả `202 Accepted`, KHÔNG phải `201 Created`. Vì:
- Order vừa tạo ở status PENDING
- Saga đang xử lý, chưa biết kết quả cuối
- Client cần poll `GET /api/orders/{id}` để biết status cuối

**UX ở client:**
- Sau khi POST → hiển thị "Order đang xử lý" + orderId
- Poll mỗi 2-3 giây hoặc nhận push notification khi status đổi
- Hoặc WebSocket / SSE cho real-time update (advanced)

---

## 11. Exception Handling

`com/demo/order/exception/GlobalExceptionHandler.java`:
```java
@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(OrderNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleNotFound(OrderNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
            .body(new ErrorResponse("ORDER_NOT_FOUND", e.getMessage()));
    }

    @ExceptionHandler(InvalidOrderStateException.class)
    public ResponseEntity<ErrorResponse> handleInvalidState(InvalidOrderStateException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
            .body(new ErrorResponse("INVALID_STATE", e.getMessage()));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException e) {
        String message = e.getBindingResult().getFieldErrors().stream()
            .map(err -> err.getField() + ": " + err.getDefaultMessage())
            .collect(Collectors.joining(", "));
        return ResponseEntity.badRequest()
            .body(new ErrorResponse("VALIDATION_ERROR", message));
    }

    @ExceptionHandler(OptimisticLockingFailureException.class)
    public ResponseEntity<ErrorResponse> handleOptimisticLock(OptimisticLockingFailureException e) {
        log.warn("Optimistic lock failure", e);
        return ResponseEntity.status(HttpStatus.CONFLICT)
            .body(new ErrorResponse("CONCURRENT_UPDATE",
                "Order was modified by another request, please retry"));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGeneric(Exception e) {
        log.error("Unexpected error", e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(new ErrorResponse("INTERNAL_ERROR", "An unexpected error occurred"));
    }

    public record ErrorResponse(String code, String message) {}
}
```

---

## 12. Cập nhật OutboxPublisher có thêm `publishCommand`

Update `messaging-lib/.../OutboxPublisher.java`:
```java
@Component
@RequiredArgsConstructor
public class OutboxPublisher {

    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public void publish(DomainEvent event, String destination) {
        save(event.getEventId(), 
             extractAggregateType(event),
             event.getAggregateId(),
             event.getEventType(),
             destination,
             event);
    }

    /**
     * Cho command/reply (không phải DomainEvent).
     * partitionKey: dùng để route message — đảm bảo cùng saga vào cùng partition.
     */
    public void publishCommand(Object commandOrReply, String destination, String partitionKey) {
        save(UUID.randomUUID(),
             "Command",
             partitionKey,
             commandOrReply.getClass().getSimpleName(),
             destination,
             commandOrReply);
    }

    private void save(UUID id, String aggregateType, String aggregateId,
                      String eventType, String destination, Object payload) {
        try {
            OutboxEvent outbox = OutboxEvent.builder()
                .id(id)
                .aggregateType(aggregateType)
                .aggregateId(aggregateId)
                .eventType(eventType)
                .destination(destination)
                .payload(objectMapper.writeValueAsString(payload))
                .createdAt(Instant.now())
                .retryCount(0)
                .build();
            outboxRepository.save(outbox);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize", e);
        }
    }

    // ...extractAggregateType giữ nguyên
}
```

---

## 13. Test thủ công end-to-end

### 13.1. Start hạ tầng + order-service
```bash
docker compose up -d
bash infra/kafka/create-topics.sh
mvn spring-boot:run -pl order-service
```

### 13.2. Tạo order
```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "customer-123",
    "items": [
      { "productId": "product-A", "quantity": 2, "unitPrice": 50.00 },
      { "productId": "product-B", "quantity": 1, "unitPrice": 30.00 }
    ]
  }'
```

Response (202 Accepted):
```json
{
  "id": "7b3e1a8d-...",
  "customerId": "customer-123",
  "status": "PENDING",
  "totalAmount": 130.00,
  "sagaId": null,
  "items": [...],
  "createdAt": "2026-06-16T10:30:00Z"
}
```

### 13.3. Verify trong DB
```bash
docker exec -it postgres psql -U admin -d order_db

\d                    # liệt kê tables
SELECT * FROM orders;
SELECT * FROM order_items;
SELECT * FROM outbox_events;      
-- Quan sát: 1 row với processed_at = NULL (chưa publish)
-- Đợi 1-2s, query lại:
SELECT * FROM outbox_events;
-- processed_at đã có giá trị → relay đã publish
```

### 13.4. Verify Kafka đã nhận
Mở Kafka UI (http://localhost:8080):
- Topic `domain.events.order` có 1 message
- Headers: `event-id`, `event-type=OrderCreated`, `aggregate-type=Order`
- Key: orderId (partition key)
- Value: JSON payload

Hoặc CLI:
```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic domain.events.order \
  --from-beginning \
  --property print.headers=true \
  --property print.key=true
```

### 13.5. Verify command flow (sau khi Saga + các service khác chạy ở bài 10)

**Lúc này (chỉ có order-service), order sẽ kẹt ở status PENDING.** Đó là đúng — chưa có Saga Orchestrator nên không ai trigger confirm.

→ Sau bài 10 (Orchestrator) sẽ test end-to-end happy path.

### 13.6. Test idempotency thủ công

Gửi command giả lập tới Kafka:
```bash
# Tạo 1 ConfirmOrderCommand giả
COMMAND_ID=$(uuidgen)
ORDER_ID="<orderId vừa tạo>"
SAGA_ID=$(uuidgen)

# Gửi 2 lần cùng commandId
for i in 1 2; do
  docker exec -i kafka kafka-console-producer \
    --bootstrap-server kafka:29092 \
    --topic saga.commands.order \
    --property "headers=event-type:ConfirmOrder" \
    --property "parse.headers=true" \
    --property "headers.delimiter=," <<EOF
{"commandId":"$COMMAND_ID","sagaId":"$SAGA_ID","orderId":"$ORDER_ID","commandType":"ConfirmOrder"}
EOF
done
```

→ Quan sát log:
- Lần 1: "Order ... confirmed by saga ..."
- Lần 2: "Command ... already processed, skipped"

→ Verify trong DB:
- Bảng `inbox_events`: chỉ có 1 row với `commandId` đó
- Bảng `orders`: status = CONFIRMED, `version` tăng đúng 1 lần

---

## 14. Bẫy phổ biến

### Bẫy 1: Quên `@Transactional` ôm cả 2 INSERT (orders + outbox)
→ Mất ý nghĩa của Outbox. Hãy double check ở mọi method publish event.

### Bẫy 2: Gọi `kafkaTemplate.send()` trực tiếp trong business logic
→ Dual Write Problem. Phải qua Outbox.

### Bẫy 3: Ack Kafka TRƯỚC khi xử lý xong
→ Consumer crash → message mất. Phải ack **sau** transaction commit.

### Bẫy 4: Logic business ở Service thay vì Entity
```java
// Sai:
if (order.getStatus() == OrderStatus.PENDING) {
    order.setStatus(OrderStatus.CONFIRMED);
    order.setUpdatedAt(Instant.now());
}
```
→ Khó maintain, logic rải rác. Đặt trong `order.confirm()`.

### Bẫy 5: Optimistic lock thiếu retry
Nếu 2 thread cùng update order → 1 thread fail với `OptimisticLockingFailureException`. Có 2 cách:
- Throw 409 Conflict → client retry (đã làm ở GlobalExceptionHandler)
- Retry tự động bằng `@Retryable` từ Spring Retry

### Bẫy 6: Không verify trên DB sau khi test
Chạy API thấy 200 OK chưa có nghĩa là đúng. Phải check:
- Order đã INSERT chưa
- Outbox có event chưa
- Outbox đã processed chưa (sau vài giây)
- Kafka có message chưa
- (Sau khi có Saga) Inbox có command chưa, status order đã đổi chưa

### Bẫy 7: Trả về `201 Created` cho POST /orders
Order chưa "created" hoàn chỉnh — chỉ mới PENDING. Phải `202 Accepted`.

### Bẫy 8: Không log đủ context
Production debug rất khó nếu log không có `sagaId`, `orderId`, `commandId`, `eventId`. Mọi log statement đều phải có các ID này.

### Bẫy 9: Thiếu `@EntityScan` ở Application class
Outbox/Inbox entity ở module `messaging-lib`, không được scan default → JPA không nhận. Đã xử lý ở `OrderServiceApplication.java` mục 8 bài 7.

---

## 15. Gợi ý thực hành

1. Code đầy đủ theo bài này, không skip phần nào.
2. Test happy path bằng curl: tạo order, verify đủ 4 nơi (orders table, outbox table, Kafka topic, log).
3. Đo latency: từ lúc INSERT order đến lúc message xuất hiện trên Kafka (Outbox polling latency).
4. Stress test: bắn 100 request đồng thời tạo order. Verify:
   - 100 order trong DB
   - 100 event trong outbox
   - 100 message trên Kafka
   - Không duplicate, không missing
5. Cố tình tạo race condition: 2 thread cùng `handleConfirmOrder` với cùng command → confirm chỉ 1 thread thành công, thread kia skip vì inbox đã có.
6. Thử kill app giữa transaction (`kill -9` process) — restart, verify không có dữ liệu nửa vời.

---

## Checklist tự đánh giá

- [ ] Hiểu vai trò Order Service trong flow Saga
- [ ] Domain logic đặt ở Entity (Rich Domain Model)
- [ ] `@Transactional` ôm cả business save + outbox save
- [ ] OutboxPublisher dùng đúng cách (không gọi kafkaTemplate trực tiếp)
- [ ] Idempotent consumer với inbox + state-based check
- [ ] Optimistic locking với @Version
- [ ] Status transition validate ở Entity
- [ ] Kafka config: acks=all, idempotent producer, manual ack, DLQ
- [ ] REST API trả 202 Accepted cho async flow
- [ ] Exception handling với GlobalExceptionHandler
- [ ] Test thủ công verify đủ DB + Outbox + Kafka + Inbox + Status

---

**Bài tiếp theo:** `09 - Implement Inventory, Payment, Shipping services` — 3 service còn lại theo cùng pattern.
