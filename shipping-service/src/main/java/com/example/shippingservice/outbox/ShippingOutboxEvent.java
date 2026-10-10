package com.example.shippingservice.outbox;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "shipping_outbox_events")
public class ShippingOutboxEvent {

    @Id
    private UUID eventId;

    private UUID orderId;

    private UUID shipmentId;

    private UUID correlationId;

    private String eventType;

    private Instant createdAt;

    private boolean published;

    protected ShippingOutboxEvent() {}

    public ShippingOutboxEvent(
            UUID orderId,
            UUID shipmentId,
            UUID correlationId
    ) {
        this.eventId = UUID.randomUUID();
        this.orderId = orderId;
        this.shipmentId = shipmentId;
        this.correlationId = correlationId;
        this.eventType = "ShipmentCreated";
        this.createdAt = Instant.now();
        this.published = false;
    }

    public UUID getEventId() {
        return eventId;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public UUID getShipmentId() {
        return shipmentId;
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