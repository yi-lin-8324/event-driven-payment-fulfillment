package com.example.paymentservice.outbox;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "payment_refund_outbox_events")
public class PaymentRefundOutboxEvent {

    @Id
    private UUID eventId;

    private UUID orderId;
    private UUID correlationId;
    private String eventType;
    private Instant createdAt;
    private UUID paymentId;
    private boolean published;

    protected PaymentRefundOutboxEvent() {
    }

    public PaymentRefundOutboxEvent(
            UUID orderId,
            UUID paymentId,
            UUID correlationId
    ) {
        this.eventId = UUID.randomUUID();
        this.orderId = orderId;
        this.paymentId = paymentId;
        this.correlationId = correlationId;
        this.eventType = "PaymentRefunded";
        this.createdAt = Instant.now();
        this.published = false;
    }

    public UUID getEventId() {
        return eventId;
    }

    public UUID getOrderId() {
        return orderId;
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

    public UUID getPaymentId() {
        return paymentId;
    }

    public boolean isPublished() {
        return published;
    }

    public void markPublished() {
        this.published = true;
    }
}