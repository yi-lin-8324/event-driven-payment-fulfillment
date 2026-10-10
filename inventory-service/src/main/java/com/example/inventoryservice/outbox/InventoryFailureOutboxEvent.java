package com.example.inventoryservice.outbox;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "inventory_failure_outbox_events")
public class InventoryFailureOutboxEvent {

    @Id
    private UUID eventId;

    private UUID orderId;
    private UUID correlationId;
    private String eventType;
    private Instant createdAt;
    private UUID paymentId;
    private String reason;
    private boolean published;

    protected InventoryFailureOutboxEvent() {
    }

    public InventoryFailureOutboxEvent(
            UUID orderId,
            UUID paymentId,
            UUID correlationId,
            String reason
    ) {
        this.eventId = UUID.randomUUID();
        this.orderId = orderId;
        this.paymentId = paymentId;
        this.correlationId = correlationId;
        this.eventType = "InventoryReservationFailed";
        this.createdAt = Instant.now();
        this.reason = reason;
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

    public String getReason() {
        return reason;
    }

    public boolean isPublished() {
        return published;
    }

    public void markPublished() {
        this.published = true;
    }
}