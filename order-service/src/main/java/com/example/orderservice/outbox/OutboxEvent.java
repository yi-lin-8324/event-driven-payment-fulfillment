package com.example.orderservice.outbox;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    @Id
    private UUID eventId;
    private UUID orderId;
    private BigDecimal amount;
    private UUID correlationId;
    private String eventType;
    private Instant createdAt;
    private boolean published;

    protected OutboxEvent() {
    }

    public OutboxEvent(
            UUID orderId,
            UUID correlationId,
            String eventType,
            BigDecimal amount
    ) {
        this.eventId = UUID.randomUUID();
        this.orderId = orderId;
        this.correlationId = correlationId;
        this.eventType = eventType;
        this.amount = amount;
        this.createdAt = Instant.now();
        this.published = false;
    }

    public UUID getEventId() {
        return eventId;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public UUID getCorrelationId() {
        return correlationId;
    }

    public String getEventType() {
        return eventType;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public boolean isPublished() {
        return published;
    }

    public void markPublished() {
        this.published = true;
    }
}