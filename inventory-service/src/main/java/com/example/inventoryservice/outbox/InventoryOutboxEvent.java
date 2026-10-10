package com.example.inventoryservice.outbox;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "inventory_outbox_events")
public class InventoryOutboxEvent {

    @Id
    private UUID eventId;

    private UUID orderId;

    private UUID reservationId;

    private UUID correlationId;

    private String eventType;

    private Instant createdAt;

    private boolean published;

    protected InventoryOutboxEvent() {}

    public InventoryOutboxEvent(
            UUID orderId,
            UUID reservationId,
            UUID correlationId
    ) {
        this.eventId = UUID.randomUUID();
        this.orderId = orderId;
        this.reservationId = reservationId;
        this.correlationId = correlationId;
        this.eventType = "InventoryReserved";
        this.createdAt = Instant.now();
        this.published = false;
    }

    public UUID getEventId() {
        return eventId;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public UUID getReservationId() {
        return reservationId;
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