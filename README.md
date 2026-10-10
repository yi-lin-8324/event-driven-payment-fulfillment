# Event-Driven Payment & Fulfillment System
[![Payment Service Tests](https://github.com/yi-lin-8324/event-driven-payment-fulfillment/actions/workflows/payment-tests.yml/badge.svg)](https://github.com/yi-lin-8324/event-driven-payment-fulfillment/actions/workflows/payment-tests.yml)

A local event-driven distributed system built with Java, Spring Boot, Apache Kafka, Avro, Schema Registry, PostgreSQL,
and Docker Compose.

The project demonstrates reliable asynchronous communication between services, transactional outbox, idempotent
consumers, Saga compensation, retries with dead-letter topics, schema evolution, and consumer-group scaling.

---

## Architecture

The system uses event-driven choreography.

Each service owns its own business state and communicates with other services through Kafka events instead of direct
database writes or synchronous service-to-service calls.

```text
                         ┌─────────────────┐
                         │   Order Service │
                         │  Order + Outbox │
                         └────────┬────────┘
                                  │
                                  │ OrderCreated
                                  ▼
                         ┌─────────────────┐
                         │ Payment Service │
                         │Payment + Outbox │
                         └────────┬────────┘
                                  │
                                  │ PaymentSucceeded
                                  ▼
                       ┌─────────────────────┐
                       │  Inventory Service  │
                       │ Inventory + Outbox  │
                       └──────────┬──────────┘
                                  │
                                  │ InventoryReserved
                                  ▼
                         ┌──────────────────┐
                         │ Shipping Service │
                         │Shipment + Outbox │
                         └────────┬─────────┘
                                  │
                                  │ ShipmentCreated
                                  ▼
                      ┌──────────────────────┐
                      │ Notification Service │
                      └──────────────────────┘
```

Order Service also consumes downstream Saga events so that it can update the overall order state.

```text
PaymentSucceeded
       │
       ▼
PAYMENT_AUTHORIZED

InventoryReserved
       │
       ▼
INVENTORY_RESERVED

ShipmentCreated
       │
       ▼
COMPLETED

PaymentRefunded
       │
       ▼
CANCELLED
```

Kafka is the communication backbone.

PostgreSQL stores service-owned application state and transactional outbox records.

Schema Registry stores Avro schemas used by Kafka producers and consumers.

---

## Technology Stack

- Java 21
- Spring Boot 4.1.1
- Spring Kafka
- Apache Kafka
- Confluent Schema Registry
- Apache Avro
- PostgreSQL 17
- Spring Data JPA
- Maven
- Docker Compose
- JUnit 5
- Testcontainers

---

## Services

### Order Service

Responsibilities:

- Accept new orders through HTTP
- Store orders in PostgreSQL
- Store `OrderCreated` in a transactional outbox
- Publish `OrderCreated`
- Consume Saga result events
- Track the final order state

Order states:

```text
CREATED
PAYMENT_AUTHORIZED
INVENTORY_RESERVED
COMPLETED
CANCELLED
```

The Order Service uses monotonic state transitions so that duplicate or late events cannot incorrectly move a completed
or cancelled order backward.

---

### Payment Service

Responsibilities:

- Consume `OrderCreated`
- Authorize payment
- Persist payment state
- Publish `PaymentSucceeded`
- Consume `InventoryReservationFailed`
- Refund previously authorized payments
- Publish `PaymentRefunded`

Payment states:

```text
AUTHORIZED
REFUNDED
```

The service also demonstrates:

- idempotent event handling
- transactional outbox
- retry topics
- dead-letter topic handling

---

### Inventory Service

Responsibilities:

- Consume `PaymentSucceeded`
- Reserve inventory
- Publish `InventoryReserved`

For the compensation demonstration, an order with:

```text
amount = 888.00
```

produces an intentional business failure:

```text
OUT_OF_STOCK
```

Inventory then publishes:

```text
InventoryReservationFailed
```

This starts the compensation flow.

---

### Shipping Service

Responsibilities:

- Consume `InventoryReserved`
- Create a shipment
- Publish `ShipmentCreated`

Shipping can run with multiple instances in the same Kafka consumer group.

The Docker Compose startup command runs two Shipping instances.

---

### Notification Service

Responsibilities:

- Consume `ShipmentCreated`
- Record/log the notification result

Notification is intentionally lightweight and does not maintain its own PostgreSQL state.

---

## Kafka Topics and Events

Kafka records use:

```text
key = orderId
```

Using `orderId` as the Kafka key keeps events for the same order consistently partitioned.

| Topic                          | Event                        | Producer  | Consumer             |
|--------------------------------|------------------------------|-----------|----------------------|
| `order-created`                | `OrderCreated`               | Order     | Payment              |
| `payment-succeeded`            | `PaymentSucceeded`           | Payment   | Inventory + Order    |
| `inventory-reserved`           | `InventoryReserved`          | Inventory | Shipping + Order     |
| `shipment-created`             | `ShipmentCreated`            | Shipping  | Notification + Order |
| `inventory-reservation-failed` | `InventoryReservationFailed` | Inventory | Payment              |
| `payment-refunded`             | `PaymentRefunded`            | Payment   | Order                |

The main event metadata includes values such as:

```text
eventId
orderId
correlationId
eventType
createdAt
```

Business events also contain fields specific to the event, such as:

```text
amount
paymentId
shipmentId
reason
```

Every new business fact receives a new `eventId`.

The same `correlationId` follows the business workflow so related events can be traced across services.

---

## Running the System

### Prerequisite

Docker with Docker Compose must be installed.

---

### Start the complete system

From the project root:

```bash
docker compose up --build -d --scale shipping-service=2
```

This starts:

```text
Kafka
Schema Registry
PostgreSQL
Order Service
Payment Service
Inventory Service
Shipping Service x2
Notification Service
```

Verify the containers:

```bash
docker compose ps
```

Order Service is exposed at:

```text
http://localhost:8080
```

Kafka is exposed to the host at:

```text
localhost:9092
```

Schema Registry is exposed at:

```text
localhost:8081
```

PostgreSQL is exposed at:

```text
localhost:5433
```

Inside Docker Compose, services communicate using Docker service names:

```text
Kafka           kafka:29092
Schema Registry schema-registry:8081
PostgreSQL      postgres:5432
```

---

## Stop the System

```bash
docker compose down
```

To also delete the PostgreSQL volume:

```bash
docker compose down -v
```

---

# Happy Path

Create a normal order:

```bash
curl -i \
  -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"amount":100.00}'
```

Example response:

```json
{
  "amount": 100.00,
  "orderId": "02946867-c05f-4fa4-87f9-34e38d07f1d9",
  "status": "CREATED"
}
```

The initial response is `CREATED` because downstream processing happens asynchronously.

The expected event flow is:

```text
POST /orders
     │
     ▼
Order stored
     │
     ▼
OrderCreated outbox row
     │
     ▼
OrderCreated
     │
     ▼
Payment authorized
     │
     ▼
PaymentSucceeded
     │
     ▼
Inventory reserved
     │
     ▼
InventoryReserved
     │
     ▼
Shipment created
     │
     ▼
ShipmentCreated
     │
     ├──────────────► Notification recorded
     │
     ▼
Order COMPLETED
```

A real Compose execution produced:

```text
Payment authorized
Inventory reserved
Shipment created
Notification recorded
Order status = COMPLETED
```

---

## Trace an Order

Use the returned `orderId`:

```bash
docker compose logs --since=5m \
  order-service \
  payment-service \
  inventory-service \
  shipping-service \
  notification-service \
  | grep "<ORDER_ID>"
```

This shows the same order moving across service boundaries.

---

# Saga Compensation

The project uses Saga choreography instead of a distributed database transaction.

A successful event is treated as an immutable business fact.

For example:

```text
PaymentSucceeded
```

is not deleted or reversed if a later step fails.

Instead, a new compensating business action occurs.

For this project, an amount of:

```text
888.00
```

simulates an inventory business failure.

Create the compensation test order:

```bash
curl -i \
  -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"amount":888.00}'
```

Expected flow:

```text
OrderCreated
     │
     ▼
Payment authorized
     │
     ▼
PaymentSucceeded
     │
     ▼
Inventory tries reservation
     │
     ▼
OUT_OF_STOCK
     │
     ▼
InventoryReservationFailed
     │
     ▼
Payment refunded
     │
     ▼
PaymentRefunded
     │
     ▼
Order CANCELLED
```

Shipping does not run for this order because inventory was never successfully reserved.

The resulting business history remains visible:

```text
Payment succeeded
→ inventory failed
→ payment refunded
```

instead of pretending that the original successful payment never happened.

---

# Transactional Outbox

Stateful producing services use the transactional outbox pattern.

Instead of:

```text
save database state
then
publish directly to Kafka
```

the service performs:

```text
Database transaction
├── save business state
└── save outbox event
```

Both changes commit together.

A scheduled publisher later reads unpublished outbox rows and sends them to Kafka.

Example:

```text
Order transaction
├── INSERT order
└── INSERT OrderCreated outbox row
```

After commit:

```text
Outbox publisher
     │
     ▼
Kafka
```

This avoids the dual-write failure where application state commits successfully but the Kafka event is lost.

The outbox publisher can still publish the same event more than once if it crashes after sending but before marking the
row as published.

That is why downstream idempotency is also required.

---

# Idempotent Consumers

Kafka delivery is treated as at-least-once.

A message may be delivered again if a consumer processes it successfully but its offset is not safely committed before a
failure.

Stateful consumers therefore record the incoming:

```text
eventId
```

in a processed-event table.

Conceptually:

```text
receive event
     │
     ▼
eventId already processed?
     │
 ┌───┴───┐
 │       │
Yes      No
 │       │
skip     process business effect
         +
         store outgoing outbox event
         +
         record eventId
```

Business state, outbox state, and processed-event state are written in the same local database transaction.

This provides effectively-once business effects on top of at-least-once message delivery.

The project does not claim global exactly-once processing across PostgreSQL and Kafka.

---

# Retry Topics and Dead Letter Topic

Payment demonstrates bounded Kafka retry handling.

If processing `OrderCreated` throws a technical exception, the event moves through:

```text
order-created
      │
      ▼
order-created-retry-0
      │
      ▼
order-created-retry-1
      │
      ▼
order-created-dlt
```

The configured number of attempts is:

```text
3 total attempts
```

That means:

```text
1 original delivery
+
2 retries
```

After all attempts fail, the event is sent to the dead-letter topic.

This separates:

```text
Business failure
```

from:

```text
Technical failure
```

For example:

```text
OUT_OF_STOCK
```

is a valid business result and produces `InventoryReservationFailed`.

It is not treated as a retryable technical exception.

---

# Consumer Groups and Scaling

Each logical service has its own Kafka consumer group.

Examples:

```text
payment-service
inventory-service
shipping-service
notification-service
order-service-saga
```

Shipping is started with two instances:

```bash
docker compose up -d --scale shipping-service=2 shipping-service
```

Kafka ensures that one partition is owned by only one consumer in the same group at a time.

If `inventory-reserved` currently has one partition:

```text
Shipping instance 1 -> inventory-reserved-0
Shipping instance 2 -> idle
```

The second instance is still healthy.

If the active instance disappears, Kafka performs a rebalance and can assign the partition to the remaining instance.

Scaling consumers therefore depends on the number of Kafka partitions available.

---

# Avro and Schema Registry

Kafka event values use Avro serialization.

Schema Registry stores the event schemas and allows compatibility checks between schema versions.

The project demonstrates schema evolution using `OrderCreated`.

Version 1 contained the original event fields.

Version 2 added:

```json
{
  "name": "sourceService",
  "type": [
    "null",
    "string"
  ],
  "default": null
}
```

The field is optional and has a default value, making the evolution compatible with older data.

Schema Registry compatibility was configured as:

```text
BACKWARD
```

The project also demonstrated a running consumer reading events safely across the additive schema change.

---

# Service-Owned Data

The project uses one PostgreSQL container for local simplicity.

However, services follow logical database ownership.

A service only accesses its own tables.

For example:

```text
Payment Service
    │
    └── payment-owned tables

Inventory Service
    │
    └── inventory-owned tables

Shipping Service
    │
    └── shipping-owned tables
```

Inventory does not directly modify Payment data.

Shipping does not directly modify Order data.

Cross-service state changes happen through events.

This preserves service boundaries even though one physical PostgreSQL instance is used locally.

---

# Failure Recovery

The project demonstrates several important distributed-system failure cases.

### Service temporarily unavailable

Kafka retains events.

When the consumer comes back online, it resumes from its committed offset and catches up.

---

### Duplicate delivery

An event may be delivered more than once.

Processed-event tracking prevents duplicate business effects in stateful consumers.

---

### Outbox publisher duplicate

A publisher may send an event successfully and crash before marking the outbox row as published.

The event may therefore be published again.

Downstream idempotency keeps the business result safe.

---

### Poison / repeatedly failing message

Technical processing failures move through bounded retry topics.

After the retry limit is reached, the event goes to the dead-letter topic.

---

# Testing

The project includes automated unit and integration tests.

## Unit Tests

Examples include:

```text
PaymentEntityTest
PaymentServiceTest
```

They verify behavior such as:

```text
AUTHORIZED → REFUNDED
```

and duplicate-event handling.

---

## PostgreSQL Integration Tests

Testcontainers starts a real PostgreSQL container.

Important scenarios include:

```text
authorization persists payment + outbox + processed event

transaction rollback removes all writes when one operation fails

refund persists REFUNDED payment + compensation outbox

duplicate authorization creates only one business effect
```

These tests verify the local transactional boundary.

---

## Kafka Integration Tests

A real Kafka Testcontainer is used for Payment retry testing.

The tests verify:

```text
OrderCreated
→ real Kafka broker
→ Avro deserialization
→ Spring Kafka listener
```

and:

```text
main topic
→ retry topic
→ retry topic
→ DLT
```

The Kafka test verifies the messaging boundary.

The PostgreSQL tests verify the persistence and transaction boundary.

---

# Delivery Semantics

The system intentionally uses:

```text
at-least-once message delivery
+
idempotent consumers
+
transactional outbox
```

The resulting model is:

```text
Kafka message may be delivered more than once

but

business effects are protected from being applied more than once
```

The project does not attempt exactly-once transactions across PostgreSQL and Kafka.

That would require significantly more infrastructure and is outside the scope of this project.

---

# Design Tradeoffs

### Choreography instead of a central Saga orchestrator

Services react to domain events.

Advantages:

```text
simple service coupling
no central workflow engine
easy to see the event flow
```

Tradeoff:

```text
workflow logic is distributed across services
```

---

### Transactional outbox instead of direct Kafka publishing

Advantages:

```text
database state and publish intent commit atomically
```

Tradeoff:

```text
requires an outbox table and publisher
duplicate Kafka publication is still possible
```

---

### At-least-once instead of global exactly-once

Advantages:

```text
simpler
realistic
works well with idempotent consumers
```

Tradeoff:

```text
services must explicitly handle duplicate delivery
```

---

### One PostgreSQL container

Advantages:

```text
simple local setup
easy Docker Compose environment
```

Tradeoff:

```text
not full physical database isolation
```

Logical service ownership is still maintained.

---

### Chosen retry strategy

Retries are bounded.

Technical failures are retried.

Business failures are represented as business events.

This prevents business outcomes such as `OUT_OF_STOCK` from being retried forever.

---

### Notification does not use a database

Notification currently records the result through logging.

Because it does not create an important persistent business side effect in this project, a database was not added solely
for deduplication.

A production notification system would likely persist delivery state or use another idempotency mechanism.

---

# Example End-to-End Happy Path

```text
Client
  │
  │ POST /orders
  ▼
Order Service
  │
  │ OrderCreated
  ▼
Kafka
  │
  ▼
Payment Service
  │
  │ PaymentSucceeded
  ▼
Kafka
  │
  ▼
Inventory Service
  │
  │ InventoryReserved
  ▼
Kafka
  │
  ▼
Shipping Service
  │
  │ ShipmentCreated
  ▼
Kafka
  ├──────────────► Notification Service
  │
  └──────────────► Order Service
                         │
                         ▼
                     COMPLETED
```

---

# Example Compensation Path

```text
Client
  │
  │ POST /orders amount=888
  ▼
Order Service
  │
  │ OrderCreated
  ▼
Payment Service
  │
  │ PaymentSucceeded
  ▼
Inventory Service
  │
  │ OUT_OF_STOCK
  │
  │ InventoryReservationFailed
  ▼
Payment Service
  │
  │ refund()
  │
  │ PaymentRefunded
  ▼
Order Service
  │
  ▼
CANCELLED
```

No shipment is created.

---

# Important Concepts Demonstrated

```text
Event-driven architecture
Kafka producers and consumers
Kafka consumer groups
Partition-based scaling
orderId as message key
Transactional outbox
At-least-once delivery
Idempotent consumers
Saga choreography
Compensating transactions
Retry topics
Dead-letter topic
Avro serialization
Schema Registry
Schema evolution
Service-owned data
Failure recovery
Docker Compose deployment
Integration testing with Testcontainers
```

---

# Project Scope

This project is intentionally designed as a focused distributed-systems demonstration.

It does not attempt to implement:

```text
Kubernetes deployment
cloud infrastructure
multi-region architecture
full observability platform
Debezium CDC
Kafka Streams
CQRS
event sourcing
generic Saga framework
global exactly-once transactions
production authentication / authorization
production commerce functionality
```

Those features would add significant complexity without improving the core learning goals of this project.

The main goal is to demonstrate and understand reliable event-driven service communication, consistency, recovery, and
Saga compensation.
