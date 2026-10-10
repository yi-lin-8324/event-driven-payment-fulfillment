package com.example.paymentservice.outbox;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "payment_outbox_events")
public class PaymentOutboxEvent {

    @Id
    private UUID eventId;

    private UUID orderId;

    private UUID paymentId;

    private UUID correlationId;

    private String eventType;

    private Instant createdAt;

    private boolean published;

    private BigDecimal amount;

    protected PaymentOutboxEvent() {}

    public PaymentOutboxEvent(
            UUID orderId,
            UUID paymentId,
            UUID correlationId,
            BigDecimal amount
    ) {
        this.eventId = UUID.randomUUID();
        this.orderId = orderId;
        this.paymentId = paymentId;
        this.correlationId = correlationId;
        this.eventType = "PaymentSucceeded";
        this.createdAt = Instant.now();
        this.published = false;
        this.amount = amount;
    }

    public UUID getEventId() {
        return eventId;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public UUID getPaymentId() {
        return paymentId;
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

    public BigDecimal getAmount() {
        return amount;
    }

    public void markPublished() {
        this.published = true;
    }
}