# Setup môi trường & Project Structure

> Bài 7 trong lộ trình Transaction Microservice nâng cao.
> Mục tiêu: Khởi tạo project Spring Boot multi-module hoàn chỉnh, hạ tầng Kafka + PostgreSQL bằng Docker Compose, shared library cho Outbox/Inbox. Đây là **nền móng kỹ thuật** cho tất cả các bài code sau.

> **Lựa chọn cho demo:** **Orchestration** (Saga Orchestrator service trung tâm) — phù hợp flow 4 bước, dễ trace, dễ show ở phỏng vấn. Cuối lộ trình có bonus Choreography variant.

---

## 0. Tech stack đã chọn và lý do

| Thành phần | Lựa chọn | Lý do |
|---|---|---|
| Java | 21 (LTS) | Virtual threads, pattern matching, record |
| Framework | Spring Boot 3.3+ | Industry standard, mature ecosystem |
| Build tool | Maven | Đơn giản, dễ multi-module |
| Database | PostgreSQL 16 | Open source, mạnh, support logical replication (cho Debezium) |
| Migration | Flyway | Standard, dễ học, version-controlled schema |
| Message broker | Kafka 3.7+ (Confluent) | Industry standard cho event-driven |
| Serialization | JSON (Jackson) | Đơn giản trước; Avro/Protobuf khi production |
| Kafka client | Spring Kafka | Tích hợp tốt với Spring |
| State machine | Spring Statemachine | Đơn giản, đủ dùng cho demo |
| Container | Docker + Docker Compose | Standard cho local dev |
| Test | JUnit 5 + Testcontainers | Integration test với Kafka/Postgres thật |
| Observability | Spring Actuator + Micrometer + OpenTelemetry | Modern stack |
| Tracing UI | Jaeger | Open source, đẹp |
| Lib khác | Lombok, MapStruct | Giảm boilerplate |

**Lưu ý:** Không dùng framework Saga (Axon, Eventuate, Temporal) — vì mục tiêu là **hiểu sâu**. Khi đã hiểu, dùng framework sẽ nhanh và an toàn hơn.

---

## 1. Cấu trúc project tổng thể

```
saga-demo/
├── pom.xml                              # Parent POM
├── docker-compose.yml                   # Hạ tầng (Kafka, Postgres, Jaeger)
├── README.md
│
├── common-lib/                          # Shared DTOs, events, exceptions
│   ├── pom.xml
│   └── src/main/java/com/demo/common/
│       ├── events/                      # Event definitions (OrderCreated, ...)
│       ├── commands/                    # Command definitions
│       └── exceptions/
│
├── messaging-lib/                       # Outbox + Inbox infrastructure
│   ├── pom.xml
│   └── src/main/java/com/demo/messaging/
│       ├── outbox/
│       │   ├── OutboxEvent.java         # Entity
│       │   ├── OutboxRepository.java
│       │   ├── OutboxRelay.java         # Polling publisher
│       │   └── OutboxConfig.java
│       └── inbox/
│           ├── InboxEvent.java
│           ├── InboxRepository.java
│           └── IdempotentConsumer.java
│
├── order-service/                       # 1 trong 4 business services
│   ├── pom.xml
│   ├── src/main/java/com/demo/order/
│   │   ├── OrderServiceApplication.java
│   │   ├── api/                         # REST controllers
│   │   ├── domain/                      # Entities, business logic
│   │   ├── infra/                       # Repositories, Kafka listeners
│   │   └── config/
│   └── src/main/resources/
│       ├── application.yml
│       └── db/migration/                # Flyway SQL scripts
│
├── inventory-service/                   # Tương tự
├── payment-service/                     # Tương tự
├── shipping-service/                    # Tương tự
│
├── saga-orchestrator/                   # Saga state machine
│   ├── pom.xml
│   └── src/main/java/com/demo/saga/
│       ├── SagaOrchestratorApplication.java
│       ├── statemachine/                # State machine config
│       ├── handler/                     # Command handlers
│       └── persistence/                 # Saga log
│
└── infra/                               # Scripts, dashboards
    ├── kafka/
    │   └── create-topics.sh
    ├── postgres/
    │   └── init-databases.sql
    └── grafana/
        └── dashboards/
```

### Vì sao tách `messaging-lib` thành module riêng?
- Outbox/Inbox **giống nhau** ở mọi service → tránh duplicate code
- Khi sửa bug Outbox → chỉ sửa 1 chỗ
- Production thực tế cũng thường có internal lib như vậy

### Vì sao mỗi service có module riêng (thay vì 1 monolith)?
- Để **giống production**: mỗi service có pom, application.yml, DB riêng
- Có thể start/stop từng service độc lập
- Sẵn sàng tách thành Git repo riêng nếu cần

---

## 2. Parent POM

`saga-demo/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.demo</groupId>
    <artifactId>saga-demo-parent</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <packaging>pom</packaging>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.3.4</version>
        <relativePath/>
    </parent>

    <properties>
        <java.version>21</java.version>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
        <spring-kafka.version>3.2.4</spring-kafka.version>
        <statemachine.version>4.0.0</statemachine.version>
        <testcontainers.version>1.20.2</testcontainers.version>
        <mapstruct.version>1.6.2</mapstruct.version>
        <opentelemetry.version>1.42.0</opentelemetry.version>
    </properties>

    <modules>
        <module>common-lib</module>
        <module>messaging-lib</module>
        <module>order-service</module>
        <module>inventory-service</module>
        <module>payment-service</module>
        <module>shipping-service</module>
        <module>saga-orchestrator</module>
    </modules>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.testcontainers</groupId>
                <artifactId>testcontainers-bom</artifactId>
                <version>${testcontainers.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <build>
        <pluginManagement>
            <plugins>
                <plugin>
                    <groupId>org.springframework.boot</groupId>
                    <artifactId>spring-boot-maven-plugin</artifactId>
                </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-compiler-plugin</artifactId>
                    <configuration>
                        <annotationProcessorPaths>
                            <path>
                                <groupId>org.projectlombok</groupId>
                                <artifactId>lombok</artifactId>
                            </path>
                            <path>
                                <groupId>org.mapstruct</groupId>
                                <artifactId>mapstruct-processor</artifactId>
                                <version>${mapstruct.version}</version>
                            </path>
                        </annotationProcessorPaths>
                    </configuration>
                </plugin>
            </plugins>
        </pluginManagement>
    </build>
</project>
```

---

## 3. Docker Compose — Hạ tầng

`saga-demo/docker-compose.yml`:

```yaml
services:
  # ===== KAFKA =====
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    container_name: kafka
    depends_on: [zookeeper]
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"  # Tự tạo topic — tắt để control rõ ràng

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    depends_on: [kafka]
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092

  # ===== POSTGRESQL =====
  # 1 instance Postgres, nhưng 4 database tách rời (mỗi service 1 DB)
  # Production: thường 4 Postgres instances riêng
  postgres:
    image: postgres:16
    container_name: postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: admin
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./infra/postgres/init-databases.sql:/docker-entrypoint-initdb.d/init.sql
    command:
      - "postgres"
      - "-c"
      - "wal_level=logical"          # Cần cho Debezium sau này
      - "-c"
      - "max_replication_slots=4"
      - "-c"
      - "max_wal_senders=4"

  pgadmin:
    image: dpage/pgadmin4:latest
    container_name: pgadmin
    ports:
      - "5050:80"
    environment:
      PGADMIN_DEFAULT_EMAIL: admin@local.com
      PGADMIN_DEFAULT_PASSWORD: admin

  # ===== OBSERVABILITY =====
  jaeger:
    image: jaegertracing/all-in-one:1.60
    container_name: jaeger
    ports:
      - "16686:16686"   # UI
      - "4317:4317"     # OTLP gRPC
      - "4318:4318"     # OTLP HTTP

  prometheus:
    image: prom/prometheus:latest
    container_name: prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./infra/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml

  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin

volumes:
  postgres-data:
```

### Init script tạo 4 database

`infra/postgres/init-databases.sql`:
```sql
CREATE DATABASE order_db;
CREATE DATABASE inventory_db;
CREATE DATABASE payment_db;
CREATE DATABASE shipping_db;
CREATE DATABASE saga_db;

-- Mỗi DB cần có user riêng (production sẽ thế); demo dùng admin chung
```

### Script tạo Kafka topics

`infra/kafka/create-topics.sh`:
```bash
#!/bin/bash
set -e

KAFKA="kafka"
PORT="29092"

create_topic() {
  docker exec ${KAFKA} kafka-topics --create \
    --bootstrap-server ${KAFKA}:${PORT} \
    --replication-factor 1 \
    --partitions 3 \
    --topic "$1" \
    --if-not-exists
}

# Saga command topics (Orchestrator → Services)
create_topic "saga.commands.inventory"
create_topic "saga.commands.payment"
create_topic "saga.commands.shipping"
create_topic "saga.commands.order"

# Saga reply topics (Services → Orchestrator)
create_topic "saga.replies"

# Domain events (cho choreography hoặc external consumers)
create_topic "domain.events.order"
create_topic "domain.events.inventory"
create_topic "domain.events.payment"
create_topic "domain.events.shipping"

# Dead Letter Queue
create_topic "saga.dlq"

echo "All topics created."
```

---

## 4. Common Library

`common-lib/pom.xml`:
```xml
<project>
    <parent>
        <groupId>com.demo</groupId>
        <artifactId>saga-demo-parent</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>
    <artifactId>common-lib</artifactId>

    <dependencies>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <scope>provided</scope>
        </dependency>
    </dependencies>
</project>
```

### Event base class

`com/demo/common/events/DomainEvent.java`:
```java
public interface DomainEvent {
    UUID getEventId();        // Unique ID — dùng cho idempotency
    String getEventType();    // "OrderCreated", "PaymentCompleted"...
    String getAggregateId();  // ID của entity gốc
    Instant getOccurredAt();
    Integer getVersion();     // Schema version
}
```

### Concrete events

`com/demo/common/events/OrderCreatedEvent.java`:
```java
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreatedEvent implements DomainEvent {
    private UUID eventId;
    private String aggregateId;       // orderId
    private Instant occurredAt;
    private Integer version;
    
    // Business payload
    private String customerId;
    private List<OrderItem> items;
    private BigDecimal totalAmount;
    
    @Override
    public String getEventType() { return "OrderCreated"; }
}
```

### Commands (cho orchestration)

`com/demo/common/commands/SagaCommand.java`:
```java
public interface SagaCommand {
    UUID getCommandId();
    String getSagaId();        // Saga instance ID
    String getCommandType();
}

@Data @Builder
public class ReserveStockCommand implements SagaCommand {
    private UUID commandId;
    private String sagaId;
    private String orderId;
    private List<OrderItem> items;
    @Override
    public String getCommandType() { return "ReserveStock"; }
}
```

### Reply messages

```java
public interface SagaReply {
    UUID getReplyId();
    UUID getCommandId();       // Tham chiếu command đã reply
    String getSagaId();
    boolean isSuccess();
}

@Data @Builder
public class StockReservedReply implements SagaReply {
    private UUID replyId;
    private UUID commandId;
    private String sagaId;
    private boolean success;
    private String failureReason;  // null nếu success
    private String reservationId;  // null nếu fail
}
```

---

## 5. Messaging Library (Outbox + Inbox)

`messaging-lib/pom.xml`:
```xml
<dependencies>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-data-jpa</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.kafka</groupId>
        <artifactId>spring-kafka</artifactId>
    </dependency>
    <dependency>
        <groupId>com.demo</groupId>
        <artifactId>common-lib</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </dependency>
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
        <scope>provided</scope>
    </dependency>
</dependencies>
```

### Outbox Entity

`com/demo/messaging/outbox/OutboxEvent.java`:
```java
@Entity
@Table(name = "outbox_events")
@Data @NoArgsConstructor @AllArgsConstructor @Builder
public class OutboxEvent {
    @Id
    private UUID id;
    
    @Column(name = "aggregate_type", nullable = false)
    private String aggregateType;       // "Order", "Payment"...
    
    @Column(name = "aggregate_id", nullable = false)
    private String aggregateId;
    
    @Column(name = "event_type", nullable = false)
    private String eventType;
    
    @Column(name = "destination", nullable = false)
    private String destination;         // Kafka topic
    
    @Column(name = "payload", columnDefinition = "jsonb", nullable = false)
    private String payload;
    
    @Column(name = "created_at", nullable = false)
    private Instant createdAt;
    
    @Column(name = "processed_at")
    private Instant processedAt;
    
    @Column(name = "retry_count", nullable = false)
    private Integer retryCount = 0;
    
    @Column(name = "last_error", columnDefinition = "text")
    private String lastError;
}
```

### Outbox Repository

```java
public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {
    
    @Query(value = """
        SELECT * FROM outbox_events
        WHERE processed_at IS NULL
          AND retry_count < :maxRetries
        ORDER BY created_at ASC
        LIMIT :batchSize
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    List<OutboxEvent> findUnprocessedForPublishing(
        @Param("batchSize") int batchSize,
        @Param("maxRetries") int maxRetries);
}
```

### Outbox Relay (Polling Publisher)

```java
@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxRelay {
    
    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    
    @Value("${outbox.batch-size:100}")
    private int batchSize;
    
    @Value("${outbox.max-retries:5}")
    private int maxRetries;
    
    @Scheduled(fixedDelayString = "${outbox.poll-interval-ms:500}")
    @Transactional
    public void publishUnprocessedEvents() {
        List<OutboxEvent> events = outboxRepository
            .findUnprocessedForPublishing(batchSize, maxRetries);
        
        if (events.isEmpty()) return;
        
        log.debug("Processing {} outbox events", events.size());
        
        for (OutboxEvent event : events) {
            try {
                publish(event);
                event.setProcessedAt(Instant.now());
                outboxRepository.save(event);
            } catch (Exception e) {
                log.error("Failed to publish event {}", event.getId(), e);
                event.setRetryCount(event.getRetryCount() + 1);
                event.setLastError(e.getMessage());
                outboxRepository.save(event);
                // Tiếp tục với event sau, không break
            }
        }
    }
    
    private void publish(OutboxEvent event) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(
            event.getDestination(),
            event.getAggregateId(),    // partition key — đảm bảo order theo aggregate
            event.getPayload()
        );
        
        record.headers().add("event-id", event.getId().toString().getBytes());
        record.headers().add("event-type", event.getEventType().getBytes());
        record.headers().add("aggregate-type", event.getAggregateType().getBytes());
        
        // Block until ack — đảm bảo at-least-once
        kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);
    }
}
```

### Outbox Publisher Helper (gọi từ business service)

```java
@Component
@RequiredArgsConstructor
public class OutboxPublisher {
    
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    
    /**
     * Gọi trong @Transactional cùng với business save.
     * Đảm bảo atomicity ghi business + ghi outbox.
     */
    public void publish(DomainEvent event, String destination) {
        try {
            OutboxEvent outbox = OutboxEvent.builder()
                .id(UUID.randomUUID())
                .aggregateType(extractAggregateType(event))
                .aggregateId(event.getAggregateId())
                .eventType(event.getEventType())
                .destination(destination)
                .payload(objectMapper.writeValueAsString(event))
                .createdAt(Instant.now())
                .retryCount(0)
                .build();
                
            outboxRepository.save(outbox);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize event", e);
        }
    }
    
    private String extractAggregateType(DomainEvent event) {
        // Convention: lấy từ class name. Vd: OrderCreatedEvent → Order
        String className = event.getClass().getSimpleName();
        return className.replace("Event", "")
            .replaceAll("(Created|Updated|Deleted|Cancelled|Completed|Failed)$", "");
    }
}
```

### Inbox Entity

`com/demo/messaging/inbox/InboxEvent.java`:
```java
@Entity
@Table(name = "inbox_events")
@Data @NoArgsConstructor @AllArgsConstructor @Builder
public class InboxEvent {
    @EmbeddedId
    private InboxEventId id;
    
    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;
    
    @Column(name = "processed_at")
    private Instant processedAt;
    
    @Embeddable
    @Data @NoArgsConstructor @AllArgsConstructor
    public static class InboxEventId implements Serializable {
        private UUID eventId;
        private String consumerGroup;
    }
}
```

### Idempotent Consumer wrapper

```java
@Component
@RequiredArgsConstructor
@Slf4j
public class IdempotentConsumer {
    
    private final InboxRepository inboxRepository;
    
    /**
     * Wrap business logic với idempotency check.
     * Phải gọi TRONG @Transactional.
     */
    public boolean processOnce(UUID eventId, String consumerGroup, Runnable businessLogic) {
        InboxEvent.InboxEventId key = new InboxEvent.InboxEventId(eventId, consumerGroup);
        
        if (inboxRepository.existsById(key)) {
            log.debug("Event {} already processed by {}, skipping", eventId, consumerGroup);
            return false;
        }
        
        try {
            inboxRepository.save(InboxEvent.builder()
                .id(key)
                .receivedAt(Instant.now())
                .build());
            
            businessLogic.run();
            
            // Mark processed (optional: có thể bỏ field này, sự tồn tại trong inbox đã đủ)
            // event.setProcessedAt(Instant.now()); 
            
            return true;
        } catch (DataIntegrityViolationException e) {
            // Race condition: 2 thread cùng insert → UNIQUE constraint catch
            log.debug("Concurrent duplicate detected for {}, skipping", eventId);
            return false;
        }
    }
}
```

---

## 6. Schema chuẩn cho mỗi service

Mỗi service đều có **3 nhóm bảng**:
1. Business tables (orders, inventory, payments, shipments)
2. `outbox_events` (giống nhau ở mọi service)
3. `inbox_events` (giống nhau ở mọi service)

### Flyway migration chuẩn cho outbox + inbox

Mỗi service: `src/main/resources/db/migration/V001__create_messaging_tables.sql`
```sql
-- OUTBOX
CREATE TABLE outbox_events (
    id              UUID PRIMARY KEY,
    aggregate_type  VARCHAR(100) NOT NULL,
    aggregate_id    VARCHAR(100) NOT NULL,
    event_type      VARCHAR(100) NOT NULL,
    destination     VARCHAR(200) NOT NULL,
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at    TIMESTAMPTZ,
    retry_count     INT NOT NULL DEFAULT 0,
    last_error      TEXT
);

-- Partial index: chỉ index các row chưa publish (table nhỏ → query nhanh)
CREATE INDEX idx_outbox_unprocessed
    ON outbox_events(created_at)
    WHERE processed_at IS NULL;

-- INBOX
CREATE TABLE inbox_events (
    event_id        UUID NOT NULL,
    consumer_group  VARCHAR(100) NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at    TIMESTAMPTZ,
    PRIMARY KEY (event_id, consumer_group)
);

CREATE INDEX idx_inbox_received_at ON inbox_events(received_at);
```

### Business tables (ví dụ order-service)

`order-service/src/main/resources/db/migration/V002__create_order_tables.sql`:
```sql
CREATE TABLE orders (
    id              UUID PRIMARY KEY,
    customer_id     VARCHAR(100) NOT NULL,
    status          VARCHAR(50) NOT NULL,
    total_amount    DECIMAL(19,4) NOT NULL,
    saga_id         UUID,                       -- liên kết tới saga instance
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    version         INT NOT NULL DEFAULT 0      -- optimistic lock
);

CREATE TABLE order_items (
    id              UUID PRIMARY KEY,
    order_id        UUID NOT NULL REFERENCES orders(id),
    product_id      VARCHAR(100) NOT NULL,
    quantity        INT NOT NULL,
    unit_price      DECIMAL(19,4) NOT NULL
);

CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_saga_id ON orders(saga_id);
```

---

## 7. Application.yml chuẩn cho mỗi service

`order-service/src/main/resources/application.yml`:
```yaml
spring:
  application:
    name: order-service
    
  datasource:
    url: jdbc:postgresql://localhost:5432/order_db
    username: admin
    password: admin
    hikari:
      maximum-pool-size: 10
      
  jpa:
    hibernate:
      ddl-auto: validate       # Flyway lo schema, JPA chỉ validate
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect
        jdbc:
          time_zone: UTC
        
  flyway:
    enabled: true
    baseline-on-migrate: true
    
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: all                # Đợi tất cả replica ack (durability)
      retries: 3
      properties:
        enable.idempotence: true     # Idempotent producer trong cùng session
        max.in.flight.requests.per.connection: 5
    consumer:
      group-id: order-service
      auto-offset-reset: earliest
      enable-auto-commit: false      # Manual commit, đảm bảo at-least-once
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      properties:
        isolation.level: read_committed
    listener:
      ack-mode: manual             # Manual ack sau khi process xong
      concurrency: 3               # 3 thread consume song song

server:
  port: 8081

# Outbox config
outbox:
  poll-interval-ms: 500
  batch-size: 100
  max-retries: 5

# Observability
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
  metrics:
    export:
      prometheus:
        enabled: true
  tracing:
    sampling:
      probability: 1.0           # Trace 100% trong dev
    
otel:
  exporter:
    otlp:
      endpoint: http://localhost:4318
  service:
    name: order-service

logging:
  level:
    com.demo: DEBUG
    org.springframework.kafka: INFO
  pattern:
    correlation: "[%X{traceId:-},%X{spanId:-}]"
```

**Mapping cổng các service:**
- order-service: 8081, DB: order_db
- inventory-service: 8082, DB: inventory_db
- payment-service: 8083, DB: payment_db
- shipping-service: 8084, DB: shipping_db
- saga-orchestrator: 8085, DB: saga_db

---

## 8. Service Application skeleton

`order-service/src/main/java/com/demo/order/OrderServiceApplication.java`:
```java
@SpringBootApplication(scanBasePackages = {
    "com.demo.order",
    "com.demo.messaging"        // Quét messaging-lib
})
@EntityScan(basePackages = {
    "com.demo.order.domain",
    "com.demo.messaging.outbox",
    "com.demo.messaging.inbox"
})
@EnableJpaRepositories(basePackages = {
    "com.demo.order.infra",
    "com.demo.messaging.outbox",
    "com.demo.messaging.inbox"
})
@EnableScheduling      // Bật @Scheduled cho OutboxRelay
public class OrderServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
```

---

## 9. Khởi động lần đầu — checklist

```bash
# 1. Clone / tạo project
mkdir saga-demo && cd saga-demo

# 2. Tạo cấu trúc thư mục như mục 1
# (Sẽ chi tiết ở bài 8)

# 3. Build parent
mvn clean install -DskipTests

# 4. Start hạ tầng
docker compose up -d

# 5. Đợi 30s rồi check
docker compose ps
# Expect: tất cả containers "Up" hoặc "healthy"

# 6. Tạo Kafka topics
bash infra/kafka/create-topics.sh

# 7. Verify topics
docker exec kafka kafka-topics --list --bootstrap-server kafka:29092

# 8. Verify databases
docker exec postgres psql -U admin -c "\l"
# Expect: order_db, inventory_db, payment_db, shipping_db, saga_db

# 9. Run từng service
mvn spring-boot:run -pl order-service
# Trong terminal khác:
mvn spring-boot:run -pl inventory-service
# v.v.

# 10. Verify
curl http://localhost:8081/actuator/health
# Expect: {"status":"UP"}
```

**Các UI để monitor:**
- Kafka UI: http://localhost:8080
- pgAdmin: http://localhost:5050
- Jaeger: http://localhost:16686
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000

---

## 10. Bẫy phổ biến khi setup

### Bẫy 1: Mỗi service share chung 1 database
**Sai.** Microservices = database-per-service. Demo có thể dùng 1 PostgreSQL instance nhưng phải tách database (đã làm vậy).

### Bẫy 2: `auto.create.topics.enable=true`
Bật cái này → Kafka tự tạo topic khi consumer subscribe → topic có config mặc định (thường sai). Luôn tắt và tạo topic explicitly.

### Bẫy 3: `enable-auto-commit=true`
Kafka tự commit offset → consumer crash giữa xử lý → offset đã commit → message bị mất. Phải `false` + manual ack sau khi xử lý xong.

### Bẫy 4: `acks=1` cho producer
`acks=1` chỉ đợi leader ack. Nếu leader chết trước khi replica sync → message mất. Production phải `acks=all` + `min.insync.replicas≥2`.

### Bẫy 5: Không set `wal_level=logical` cho Postgres
Sau này dùng Debezium → cần `wal_level=logical`. Đặt sẵn từ đầu để không phải restart DB sau (đã set trong docker-compose).

### Bẫy 6: `ddl-auto=update` hoặc `create`
Spring tự sửa schema → mất control, nguy hiểm production. Phải `validate` + Flyway lo schema.

### Bẫy 7: Quên `@EntityScan` cho messaging-lib
Spring Boot chỉ scan package của Application class theo default. Outbox/Inbox entity ở module khác → cần khai báo `@EntityScan` rõ ràng.

### Bẫy 8: Đặt `concurrency` quá cao
3-5 là đẹp. Đặt 50 → thread context switch, contention DB connection pool. Phải tỉ lệ với pool size + số partition của topic.

---

## 11. Gợi ý thực hành

1. Tạo project structure theo mục 1, **không cần code**, chỉ tạo pom + folder.
2. Run `docker compose up -d`, verify tất cả container chạy.
3. Đăng nhập Kafka UI, pgAdmin, Jaeger — quen UI trước khi code.
4. Tạo 1 topic test, dùng `kafka-console-producer`/`consumer` để gửi nhận message.
5. Tạo 1 table dummy trong order_db, INSERT/SELECT — verify connectivity từ host.
6. Đọc Spring Kafka reference docs phần Producer + Consumer config.
7. Đọc PostgreSQL docs về `FOR UPDATE SKIP LOCKED` — sẽ dùng cực nhiều.

---

## Checklist tự đánh giá

- [ ] Hiểu vì sao chia thành multi-module thay vì 1 project lớn
- [ ] Vẽ được cấu trúc thư mục mà không cần xem lại
- [ ] Giải thích vai trò từng module: common-lib, messaging-lib, các service, orchestrator
- [ ] Hiểu mọi setting Kafka quan trọng: acks, idempotence, isolation, auto-commit
- [ ] Hiểu vì sao mỗi service phải có DB riêng
- [ ] Schema chuẩn outbox + inbox áp dụng được cho cả 5 service
- [ ] Hiểu vai trò `wal_level=logical` (chuẩn bị cho Debezium)
- [ ] Run được docker compose, kết nối được Kafka và Postgres từ host
- [ ] Liệt kê 8 bẫy phổ biến khi setup

---

**Bài tiếp theo:** `08 - Implement Order Service đầy đủ (REST API + Outbox + Inbox + Saga starter)` — bắt đầu code service đầu tiên, end-to-end.
