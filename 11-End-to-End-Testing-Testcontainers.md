# End-to-End Testing với Testcontainers

> Bài 11 trong lộ trình Transaction Microservice nâng cao.
> Mục tiêu: Viết test tự động cho hệ thống Saga — từ unit test domain logic, integration test từng service, đến end-to-end test cả flow. Đây là **kỹ năng bắt buộc** cho Senior Backend và một câu phỏng vấn classic.

---

## 0. Thách thức của testing distributed system

Test microservices KHÓ hơn monolith vì:

1. **Nhiều process** — phải start nhiều service mới test được flow
2. **Nhiều dependency** — DB, Kafka, external API
3. **Async + eventual consistency** — kết quả không xuất hiện ngay sau khi gọi API
4. **Network unpredictability** — flaky test nếu không xử lý timing đúng
5. **Test data isolation** — Saga 1 đụng Saga 2 nếu shared state
6. **Chậm** — start 5 service + Kafka + Postgres mất phút

→ Cần **strategy nhiều tầng**, không phải 1 loại test cho tất cả.

---

## 1. Test Pyramid cho Microservices

```
                    ▲
                   ╱ ╲              E2E Tests           (ít, chậm, expensive)
                  ╱ E ╲             1-5 test cases
                 ╱─────╲
                ╱ Integ ╲           Integration Tests   (vừa, vừa nhanh)
               ╱─────────╲          ~10-20 per service
              ╱           ╲
             ╱   Contract  ╲        Contract Tests      (vừa)
            ╱───────────────╲       1 per producer-consumer pair
           ╱                 ╲
          ╱       Unit        ╲     Unit Tests          (nhiều, nhanh, cheap)
         ╱──────────────────────╲   Hundreds per service
```

| Loại | Phạm vi | Tốc độ | Số lượng | Mục đích |
|---|---|---|---|---|
| **Unit** | 1 class | < 1ms | Hàng trăm | Domain logic, edge case |
| **Integration** | 1 service + DB + Kafka | 1-5s | 10-20 | Component cùng làm việc đúng |
| **Contract** | Cặp producer-consumer | 1-2s | Per cặp | Event schema không phá vỡ |
| **E2E** | Cả hệ thống | 30s-2min | 1-5 | Critical user journey |

**Nguyên tắc:** càng cao trong pyramid càng ít test. Mỗi E2E test là **bảo hiểm**, không phải coverage chính.

---

## 2. Testcontainers — Vũ khí chính

### 2.1. Vấn đề trước Testcontainers
- Mock DB (H2): không giống Postgres thật → bug "chạy được local, fail production"
- Mock Kafka: không có thật concurrency, partition behavior
- Embedded Kafka: chậm, flaky, khác hành vi production

### 2.2. Testcontainers là gì
Java library cho phép start **container Docker thực sự** từ JUnit test. Test với PostgreSQL/Kafka **thật**, không phải mock.

```java
@Container
static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

@Container  
static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));
```

→ Container start trước test, stop sau test. Có thể share giữa nhiều test class.

### 2.3. Setup dependency

`order-service/pom.xml` (test scope):
```xml
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
<dependency>
    <groupId>org.awaitility</groupId>
    <artifactId>awaitility</artifactId>
    <scope>test</scope>
</dependency>
```

---

## 3. Unit Tests — Domain Logic

**Không cần Spring, không cần DB.** Chỉ test pure logic.

### 3.1. Test Order entity

`order-service/src/test/.../OrderTest.java`:
```java
class OrderTest {

    @Test
    void create_shouldCalculateTotalCorrectly() {
        // given
        List<OrderItem> items = List.of(
            OrderItem.builder()
                .productId("A").quantity(2).unitPrice(new BigDecimal("50.00")).build(),
            OrderItem.builder()
                .productId("B").quantity(1).unitPrice(new BigDecimal("30.00")).build()
        );

        // when
        Order order = Order.create("customer-1", items);

        // then
        assertThat(order.getTotalAmount()).isEqualByComparingTo("130.00");
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(order.getId()).isNotNull();
    }

    @Test
    void confirm_fromPending_shouldSucceed() {
        Order order = Order.create("c1", anyItems());
        UUID sagaId = UUID.randomUUID();

        order.confirm(sagaId);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(order.getSagaId()).isEqualTo(sagaId);
    }

    @Test
    void confirm_alreadyConfirmed_shouldThrow() {
        Order order = Order.create("c1", anyItems());
        order.confirm(UUID.randomUUID());

        assertThatThrownBy(() -> order.confirm(UUID.randomUUID()))
            .isInstanceOf(InvalidOrderStateException.class)
            .hasMessageContaining("CONFIRMED");
    }

    @Test
    void cancel_alreadyCancelled_shouldBeIdempotent() {
        Order order = Order.create("c1", anyItems());
        order.cancel(UUID.randomUUID(), "test");
        
        // Lần 2 không throw — idempotent
        order.cancel(UUID.randomUUID(), "test");
        
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
    }

    private List<OrderItem> anyItems() {
        return List.of(OrderItem.builder()
            .productId("X").quantity(1).unitPrice(BigDecimal.TEN).build());
    }
}
```

### 3.2. Test Inventory reservation

```java
class InventoryTest {

    @Test
    void reserve_withSufficientStock_shouldDecrementAvailable() {
        Inventory inv = Inventory.builder()
            .productId("A")
            .availableQuantity(100)
            .reservedQuantity(0)
            .build();

        inv.reserve(20);

        assertThat(inv.getAvailableQuantity()).isEqualTo(80);
        assertThat(inv.getReservedQuantity()).isEqualTo(20);
    }

    @Test
    void reserve_withInsufficientStock_shouldThrow() {
        Inventory inv = Inventory.builder()
            .productId("A").availableQuantity(10).reservedQuantity(0)
            .build();

        assertThatThrownBy(() -> inv.reserve(20))
            .isInstanceOf(InsufficientStockException.class);
    }

    @Test
    void releaseAfterReserve_shouldRestoreOriginal() {
        Inventory inv = Inventory.builder()
            .productId("A").availableQuantity(100).reservedQuantity(0)
            .build();

        inv.reserve(30);
        inv.release(30);

        assertThat(inv.getAvailableQuantity()).isEqualTo(100);
        assertThat(inv.getReservedQuantity()).isEqualTo(0);
    }
}
```

### 3.3. Test SagaState transitions

```java
class SagaStateTransitionTest {

    @ParameterizedTest
    @MethodSource("validTransitions")
    void validTransitions_shouldBeAllowed(SagaState from, SagaState to) {
        SagaInstance saga = sagaInState(from);
        
        saga.transitionTo(to);
        
        assertThat(saga.getCurrentState()).isEqualTo(to);
    }

    @ParameterizedTest
    @MethodSource("invalidTransitions")
    void invalidTransitions_shouldThrow(SagaState from, SagaState to) {
        SagaInstance saga = sagaInState(from);
        
        assertThatThrownBy(() -> saga.transitionTo(to))
            .isInstanceOf(InvalidSagaTransitionException.class);
    }

    static Stream<Arguments> validTransitions() {
        return Stream.of(
            Arguments.of(STARTED, AWAITING_STOCK_RESERVATION),
            Arguments.of(AWAITING_STOCK_RESERVATION, AWAITING_PAYMENT),
            Arguments.of(AWAITING_STOCK_RESERVATION, AWAITING_ORDER_CANCELLATION),
            Arguments.of(AWAITING_PAYMENT, AWAITING_SHIPMENT),
            // ... đủ
        );
    }

    static Stream<Arguments> invalidTransitions() {
        return Stream.of(
            Arguments.of(COMPLETED, AWAITING_PAYMENT),
            Arguments.of(FAILED, COMPLETED),
            Arguments.of(STARTED, COMPLETED)
        );
    }
}
```

---

## 4. Integration Tests — Single Service

Test 1 service với DB + Kafka **thật** (qua Testcontainers). Verify được:
- Save DB + Outbox event atomic
- Kafka producer/consumer hoạt động
- Idempotency check đúng

### 4.1. Base test class — Shared infrastructure

`order-service/src/test/.../IntegrationTestBase.java`:
```java
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = OrderServiceApplication.class
)
@Testcontainers
@ActiveProfiles("test")
public abstract class IntegrationTestBase {

    // Static = shared between all test classes inheriting this
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
        DockerImageName.parse("postgres:16"))
        .withDatabaseName("order_db_test")
        .withUsername("test")
        .withPassword("test")
        .withReuse(true);  // Reuse container across runs (faster local dev)

    static KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.0"))
        .withReuse(true);

    static {
        postgres.start();
        kafka.start();
    }

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        
        // Tăng tốc test — không cần outbox poll 500ms
        registry.add("outbox.poll-interval-ms", () -> "100");
    }

    @Autowired protected TestRestTemplate restTemplate;
    @Autowired protected OrderRepository orderRepository;
    @Autowired protected OutboxRepository outboxRepository;
    @Autowired protected DataSource dataSource;

    @BeforeEach
    void cleanDatabase() {
        // Clean state giữa các test
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE order_items, orders, outbox_events, inbox_events CASCADE");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
```

**Lưu ý quan trọng:**
- `static` container + `withReuse(true)` → container chỉ start 1 lần cho tất cả test → nhanh
- `@DynamicPropertySource` → inject URL container vào Spring properties
- `cleanDatabase()` trong `@BeforeEach` → test isolation

### 4.2. Application-test.yml

`src/test/resources/application-test.yml`:
```yaml
spring:
  jpa:
    hibernate:
      ddl-auto: validate
  flyway:
    enabled: true

# Logging gọn cho test
logging:
  level:
    org.hibernate.SQL: WARN
    com.demo: INFO
```

### 4.3. Integration test cho createOrder

`OrderServiceIntegrationTest.java`:
```java
class OrderServiceIntegrationTest extends IntegrationTestBase {

    @Test
    void createOrder_shouldSaveOrderAndOutboxEvent_atomically() {
        // given
        CreateOrderRequest request = CreateOrderRequest.builder()
            .customerId("customer-1")
            .items(List.of(
                OrderItemDto.builder()
                    .productId("product-A")
                    .quantity(2)
                    .unitPrice(new BigDecimal("50.00"))
                    .build()
            ))
            .build();

        // when
        ResponseEntity<OrderResponse> response = restTemplate.postForEntity(
            "/api/orders", request, OrderResponse.class);

        // then — HTTP layer
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody()).isNotNull();
        UUID orderId = response.getBody().getId();

        // then — Database layer
        Order order = orderRepository.findById(orderId).orElseThrow();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(order.getTotalAmount()).isEqualByComparingTo("100.00");
        assertThat(order.getItems()).hasSize(1);

        // then — Outbox layer (event đã được tạo cùng transaction)
        List<OutboxEvent> outboxEvents = outboxRepository.findAll();
        assertThat(outboxEvents).hasSize(1);
        OutboxEvent event = outboxEvents.get(0);
        assertThat(event.getEventType()).isEqualTo("OrderCreated");
        assertThat(event.getAggregateId()).isEqualTo(orderId.toString());
        assertThat(event.getDestination()).isEqualTo("domain.events.order");
    }

    @Test
    void createOrder_shouldEventuallyPublishToKafka() {
        // given
        UUID orderId = createTestOrder();

        // then — đợi outbox relay publish (poll-interval=100ms ở test)
        await()
            .atMost(5, TimeUnit.SECONDS)
            .pollInterval(200, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                OutboxEvent event = outboxRepository.findAll().get(0);
                assertThat(event.getProcessedAt()).isNotNull();
            });
    }

    @Test
    void getOrder_shouldReturnOrderDetails() {
        UUID orderId = createTestOrder();

        ResponseEntity<OrderResponse> response = restTemplate.getForEntity(
            "/api/orders/" + orderId, OrderResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().getId()).isEqualTo(orderId);
        assertThat(response.getBody().getStatus()).isEqualTo("PENDING");
    }

    @Test
    void getOrder_nonExistent_shouldReturn404() {
        UUID randomId = UUID.randomUUID();

        ResponseEntity<String> response = restTemplate.getForEntity(
            "/api/orders/" + randomId, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void createOrder_concurrent100Requests_shouldAllSucceed() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(20);
        List<Future<UUID>> futures = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            futures.add(executor.submit(() -> createTestOrder()));
        }

        List<UUID> orderIds = new ArrayList<>();
        for (Future<UUID> f : futures) {
            orderIds.add(f.get(10, TimeUnit.SECONDS));
        }

        // Verify all unique, all saved
        assertThat(orderIds).hasSize(100).doesNotHaveDuplicates();
        assertThat(orderRepository.count()).isEqualTo(100);
        assertThat(outboxRepository.count()).isEqualTo(100);

        executor.shutdown();
    }

    private UUID createTestOrder() {
        CreateOrderRequest request = CreateOrderRequest.builder()
            .customerId("c-" + UUID.randomUUID())
            .items(List.of(OrderItemDto.builder()
                .productId("product-A").quantity(1)
                .unitPrice(BigDecimal.TEN).build()))
            .build();
        return restTemplate.postForObject(
            "/api/orders", request, OrderResponse.class).getId();
    }
}
```

### 4.4. Test Idempotent Consumer

```java
class SagaCommandHandlerIntegrationTest extends IntegrationTestBase {

    @Autowired SagaCommandHandler handler;
    @Autowired InboxRepository inboxRepository;
    
    @Test
    void handleConfirmOrder_calledTwiceWithSameCommandId_shouldOnlyProcessOnce() {
        // given
        UUID orderId = createPendingOrder();
        UUID commandId = UUID.randomUUID();
        UUID sagaId = UUID.randomUUID();
        
        ConfirmOrderCommand cmd = ConfirmOrderCommand.builder()
            .commandId(commandId)
            .sagaId(sagaId.toString())
            .orderId(orderId.toString())
            .build();

        // when — gọi 2 lần
        handler.handleConfirmOrder(cmd);
        handler.handleConfirmOrder(cmd);

        // then
        Order order = orderRepository.findById(orderId).orElseThrow();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(order.getVersion()).isEqualTo(1);  // chỉ update 1 lần
        
        // Inbox chỉ có 1 record
        assertThat(inboxRepository.count()).isEqualTo(1);
    }
}
```

---

## 5. End-to-End Tests — Cả hệ thống

Đây là phần khó nhất. Có 3 cách:

### 5.1. Cách A: Spring MultipleContext (1 JVM, 5 contexts)
Phức tạp, dễ conflict bean. Không khuyên dùng.

### 5.2. Cách B: Docker Compose Testcontainers (5 process riêng)
**Khuyên dùng cho demo.** Build Docker image của mỗi service, start qua Testcontainers Docker Compose module.

### 5.3. Cách C: External test runner
Start services manual, test chạy từ ngoài qua HTTP. Phù hợp CI.

### Implement Cách B

#### Build Docker image cho từng service

`order-service/Dockerfile`:
```dockerfile
FROM eclipse-temurin:21-jre-alpine
COPY target/order-service-*.jar app.jar
ENTRYPOINT ["java", "-jar", "/app.jar"]
```

Build:
```bash
mvn clean package -DskipTests
docker build -t saga-demo/order-service:test ./order-service
# ... tương tự 4 service còn lại
```

#### Docker Compose cho E2E test

`saga-e2e-tests/src/test/resources/docker-compose-e2e.yml`:
```yaml
services:
  postgres-e2e:
    image: postgres:16
    environment:
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: admin
    command: ["postgres", "-c", "wal_level=logical"]
    volumes:
      - ./init-databases.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U admin"]
      interval: 2s
      retries: 10

  kafka-e2e:
    image: confluentinc/cp-kafka:7.6.0
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      # ... (đầy đủ KRaft config)
    healthcheck:
      test: ["CMD-SHELL", "kafka-topics --bootstrap-server kafka-e2e:9092 --list"]
      interval: 5s
      retries: 10

  order-service:
    image: saga-demo/order-service:test
    depends_on:
      postgres-e2e: { condition: service_healthy }
      kafka-e2e:    { condition: service_healthy }
    environment:
      SPRING_DATASOURCE_URL: jdbc:postgresql://postgres-e2e:5432/order_db
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka-e2e:9092
    ports:
      - "8081"  # expose random host port

  # ... inventory-service, payment-service, shipping-service, saga-orchestrator
```

#### E2E test class

`saga-e2e-tests/src/test/java/E2ESagaTest.java`:
```java
@Testcontainers
class E2ESagaTest {

    @Container
    static ComposeContainer environment = new ComposeContainer(
            new File("src/test/resources/docker-compose-e2e.yml"))
        .withExposedService("order-service", 8081, 
            Wait.forHttp("/actuator/health").forStatusCode(200))
        .withExposedService("saga-orchestrator", 8085, 
            Wait.forHttp("/actuator/health").forStatusCode(200))
        // ... wait for all services
        .withStartupTimeout(Duration.ofMinutes(3));

    private RestTemplate restTemplate = new RestTemplate();
    private String orderServiceUrl;
    private String sagaServiceUrl;

    @BeforeEach
    void setup() {
        orderServiceUrl = "http://" + environment.getServiceHost("order-service", 8081)
            + ":" + environment.getServicePort("order-service", 8081);
        sagaServiceUrl = "http://" + environment.getServiceHost("saga-orchestrator", 8085)
            + ":" + environment.getServicePort("saga-orchestrator", 8085);
        
        // Seed inventory data
        seedInventory();
    }

    @Test
    void happyPath_orderShouldEventuallyBeConfirmed() {
        // given — Order request
        CreateOrderRequest request = CreateOrderRequest.builder()
            .customerId("e2e-customer-1")
            .items(List.of(OrderItemDto.builder()
                .productId("product-A").quantity(2)
                .unitPrice(new BigDecimal("50.00")).build()))
            .build();

        // when — POST tạo order
        ResponseEntity<OrderResponse> createResponse = restTemplate.postForEntity(
            orderServiceUrl + "/api/orders", request, OrderResponse.class);

        assertThat(createResponse.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        UUID orderId = createResponse.getBody().getId();

        // then — đợi Saga complete (eventual consistency)
        await()
            .atMost(30, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                OrderResponse order = restTemplate.getForObject(
                    orderServiceUrl + "/api/orders/" + orderId, OrderResponse.class);
                assertThat(order.getStatus()).isEqualTo("CONFIRMED");
            });

        // then — verify saga state
        SagaDetailResponse saga = restTemplate.getForObject(
            sagaServiceUrl + "/api/sagas/by-order/" + orderId, SagaDetailResponse.class);
        assertThat(saga.getCurrentState()).isEqualTo("COMPLETED");
        
        // Verify saga went through all expected steps
        List<String> stepStates = saga.getSteps().stream()
            .map(SagaStep::getCommandType).toList();
        assertThat(stepStates).contains(
            "AWAITING_STOCK_RESERVATION",
            "AWAITING_PAYMENT",
            "AWAITING_SHIPMENT",
            "AWAITING_ORDER_CONFIRMATION"
        );
    }

    @Test
    void stockInsufficient_orderShouldBeCancelled() {
        // given — order vượt quá stock
        CreateOrderRequest request = CreateOrderRequest.builder()
            .customerId("e2e-customer-2")
            .items(List.of(OrderItemDto.builder()
                .productId("product-A").quantity(99999)  // quá nhiều
                .unitPrice(new BigDecimal("50.00")).build()))
            .build();

        // when
        UUID orderId = restTemplate.postForObject(
            orderServiceUrl + "/api/orders", request, OrderResponse.class).getId();

        // then — đợi Saga fail
        await().atMost(30, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                OrderResponse order = restTemplate.getForObject(
                    orderServiceUrl + "/api/orders/" + orderId, OrderResponse.class);
                assertThat(order.getStatus()).isEqualTo("CANCELLED");
            });

        // Verify saga ended in FAILED state
        SagaDetailResponse saga = restTemplate.getForObject(
            sagaServiceUrl + "/api/sagas/by-order/" + orderId, SagaDetailResponse.class);
        assertThat(saga.getCurrentState()).isEqualTo("FAILED");
        assertThat(saga.getFailureReason()).contains("Insufficient stock");
    }

    @Test
    void paymentFails_inventoryShouldBeReleased() {
        // given — force payment fail (toggle qua test endpoint hoặc env var)
        setPaymentFailureRate(1.0);
        try {
            int initialStock = getInventoryAvailable("product-A");
            
            // when
            UUID orderId = createOrder("product-A", 5);

            // then — order CANCELLED
            await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
                OrderResponse order = getOrder(orderId);
                assertThat(order.getStatus()).isEqualTo("CANCELLED");
            });

            // then — inventory được trả về đầy đủ (compensation OK)
            int finalStock = getInventoryAvailable("product-A");
            assertThat(finalStock).isEqualTo(initialStock);
            
        } finally {
            setPaymentFailureRate(0.0);  // reset
        }
    }

    // ... helper methods
}
```

---

## 6. Awaitility — Pattern test async đúng cách

### ❌ ANTI-PATTERN

```java
Thread.sleep(5000);
assertThat(orderStatus).isEqualTo("CONFIRMED");  // flaky!
```

`Thread.sleep` là kẻ thù của test:
- Quá ngắn → flaky
- Quá dài → chậm
- Không phản hồi với hệ thống thực

### ✅ ĐÚNG

```java
await()
    .atMost(30, TimeUnit.SECONDS)        // tối đa 30s
    .pollInterval(500, TimeUnit.MILLISECONDS)  // check mỗi 500ms
    .pollDelay(100, TimeUnit.MILLISECONDS)     // đợi 100ms trước lần check đầu
    .untilAsserted(() -> {
        assertThat(getOrderStatus()).isEqualTo("CONFIRMED");
    });
```

**Lợi ích:**
- Pass ngay khi điều kiện đúng → nhanh
- Fail rõ ràng với assertion error nếu timeout
- Cấu hình được per-test

### Patterns thường dùng

```java
// Đợi điều kiện đơn giản
await().until(() -> orderRepository.findById(id).isPresent());

// Đợi với assertion
await().untilAsserted(() -> 
    assertThat(getOrder(id).getStatus()).isEqualTo("CONFIRMED"));

// Đợi với conditional
await().until(
    () -> getInventoryAvailable("A"), 
    available -> available >= 95
);

// Custom config cho test chậm
await()
    .atMost(2, TimeUnit.MINUTES)
    .pollInterval(2, TimeUnit.SECONDS)
    .ignoreException(IOException.class)
    .untilAsserted(...);
```

---

## 7. Test Data Builders

Tránh code lặp khi tạo test data:

```java
public class TestDataBuilders {

    public static class OrderBuilder {
        private String customerId = "default-customer";
        private List<OrderItemDto> items = List.of(defaultItem());

        public OrderBuilder forCustomer(String id) {
            this.customerId = id;
            return this;
        }

        public OrderBuilder withItem(String productId, int qty, BigDecimal price) {
            this.items = new ArrayList<>(this.items);
            this.items.add(OrderItemDto.builder()
                .productId(productId).quantity(qty).unitPrice(price).build());
            return this;
        }

        public CreateOrderRequest build() {
            return CreateOrderRequest.builder()
                .customerId(customerId).items(items).build();
        }

        private static OrderItemDto defaultItem() {
            return OrderItemDto.builder()
                .productId("product-A").quantity(1)
                .unitPrice(BigDecimal.TEN).build();
        }
    }

    public static OrderBuilder anOrder() {
        return new OrderBuilder();
    }
}
```

Sử dụng:
```java
@Test
void test() {
    CreateOrderRequest req = anOrder()
        .forCustomer("c-1")
        .withItem("product-B", 3, new BigDecimal("20.00"))
        .build();
    // ...
}
```

→ **Đọc test như đọc tài liệu**, không phải đọc code.

---

## 8. Contract Testing (advanced — preview)

Khi service A publish event, service B consume. Vấn đề: A đổi schema → B break, nhưng không có warning.

**Contract test** kiểm tra: producer publish event đúng schema mà consumer expect.

Tool phổ biến: **Pact** (consumer-driven contract testing).

```java
// Consumer side (inventory-service) declares: "Tôi expect event này"
@Pact(consumer = "inventory-service", provider = "order-service")
public RequestResponsePact orderCreatedEventPact(PactDslWithProvider builder) {
    return builder
        .given("OrderCreatedEvent is published")
        .uponReceiving("an OrderCreatedEvent")
        .body(LambdaDsl.newJsonBody(o -> o
            .uuid("eventId")
            .stringType("aggregateId")
            .stringValue("eventType", "OrderCreated")
            .array("items", a -> a.object(item -> item
                .stringType("productId")
                .numberType("quantity")
            ))
        ).build())
        .toPact();
}
```

→ Sau test, Pact tạo contract file. Producer (order-service) chạy verification → fail nếu schema không match.

→ Advanced topic, bài này chỉ giới thiệu. Production microservices nên có.

---

## 9. CI/CD considerations

### 9.1. Test layers chạy ở đâu

| Layer | Local | CI per-commit | CI nightly |
|---|---|---|---|
| Unit | ✓ | ✓ | ✓ |
| Integration | ✓ | ✓ | ✓ |
| Contract | optional | ✓ | ✓ |
| E2E | optional | quick subset | ✓ full |

**Lý do tách:** E2E chậm (1-2 phút mỗi test), không thể chạy hết cho mỗi commit. Chỉ chạy critical path quick subset + full suite nightly.

### 9.2. Test parallelization

```xml
<!-- maven-surefire-plugin -->
<configuration>
    <parallel>classes</parallel>
    <threadCount>4</threadCount>
    <forkCount>2</forkCount>
    <reuseForks>true</reuseForks>
</configuration>
```

Cẩn thận: container shared, DB cleanup cần lock.

### 9.3. Docker layer caching trong CI
```yaml
# GitHub Actions example
- uses: docker/setup-buildx-action@v3
- uses: docker/build-push-action@v5
  with:
    context: ./order-service
    cache-from: type=gha
    cache-to: type=gha,mode=max
```

---

## 10. Bẫy phổ biến

### Bẫy 1: `Thread.sleep` trong test
Đã nói ở mục 6. Bao giờ cũng dùng `Awaitility`.

### Bẫy 2: Không clean state giữa test
Test A tạo order ở status A → Test B chạy → query thấy order cũ → assert sai.
→ `@BeforeEach` TRUNCATE tables.
→ Hoặc dùng `@DirtiesContext` (chậm).

### Bẫy 3: Container start mỗi class → chậm
`@Container` non-static → start mỗi class. Đổi sang static + `withReuse(true)`.

### Bẫy 4: Test phụ thuộc thứ tự
JUnit không guarantee order. Mỗi test phải self-contained.

### Bẫy 5: Skip cleanup cho Kafka topic
Test A consume offset 5, Test B chạy → vẫn ở offset 5 → message cũ pop ra.
→ Tạo topic mới mỗi test, hoặc reset offset, hoặc dùng unique consumer group.

### Bẫy 6: Test "vô tình" pass do timing
Order chuyển PENDING → trong 100ms test query thấy PENDING → assert PENDING → pass.
Nhưng intent là test "Saga đã chạy chưa".
→ Đợi tới state cuối cùng (CONFIRMED), không state trung gian.

### Bẫy 7: Mock quá nhiều ở integration test
Mock Kafka mất ý nghĩa test. Mock external API thì OK.
→ Integration test: real DB, real Kafka, mock external (payment gateway).

### Bẫy 8: Slow test → ignore
"Test chậm quá, skip cho nhanh" → dần dần ai cũng skip → bug lọt prod.
→ Tách E2E vào CI riêng, đừng skip.

### Bẫy 9: Test dependency thứ tự outbox publish
Outbox poll mỗi 100-500ms. Test query Kafka ngay → chưa có message → fail.
→ Awaitility đợi đến khi outbox.processedAt != null.

### Bẫy 10: Container Docker không stop sau test fail
JUnit `@Container` static + Ctrl+C → container không stop → port bị giữ.
→ Run `docker ps -a` periodic, dùng `--rm` flag, hoặc Ryuk (Testcontainers tự cleanup).

---

## 11. Gợi ý thực hành

1. Viết unit test cho **toàn bộ** domain entities (Order, Inventory, Payment, Shipment, SagaInstance). Mục tiêu coverage > 90% cho domain layer.
2. Viết integration test cho mỗi service (5-10 test/service). Test:
   - Happy path
   - Idempotency (gọi 2 lần)
   - Error handling
   - Concurrent requests
3. Viết E2E test cho 3 scenario:
   - Happy path
   - Stock insufficient
   - Payment fail
4. Đo thời gian test:
   - Unit: <1s tổng
   - Integration: <30s tổng per service
   - E2E: <2min tổng
5. Bật `withReuse(true)` cho local dev. CI thì để false (clean isolation).
6. Setup GitHub Actions chạy unit + integration mỗi PR, E2E nightly.

---

## Checklist tự đánh giá

- [ ] Vẽ được test pyramid và biết mỗi layer test gì
- [ ] Setup Testcontainers cho PostgreSQL + Kafka
- [ ] Viết base class chia sẻ container giữa các test
- [ ] Sử dụng `@DynamicPropertySource` inject container URL
- [ ] Clean state giữa các test (`@BeforeEach`)
- [ ] Dùng Awaitility thay vì `Thread.sleep`
- [ ] Test data builder cho code test dễ đọc
- [ ] Viết E2E test với Docker Compose Testcontainers
- [ ] Hiểu khi nào mock vs khi nào dùng real (DB, Kafka)
- [ ] Phân biệt CI strategy cho unit/integration/E2E
- [ ] Liệt kê 10 bẫy testing và cách tránh
- [ ] Đo và optimize được test thời gian

---

**Bài tiếp theo:** `12 - Failure Injection & Chaos Engineering nhẹ` — kill Kafka, kill DB, kill service giữa flow. Xác minh hệ thống recover đúng.
