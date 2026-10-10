package com.example.shippingservice.shipping;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.util.UUID;

@Entity
@Table(name = "shipments")
public class ShipmentEntity {

    @Id
    private UUID shipmentId;

    private UUID orderId;

    @Enumerated(EnumType.STRING)
    private ShipmentStatus status;

    protected ShipmentEntity() {}

    public ShipmentEntity(UUID orderId) {
        this.shipmentId = UUID.randomUUID();
        this.orderId = orderId;
        this.status = ShipmentStatus.CREATED;
    }

    public UUID getShipmentId() {
        return shipmentId;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public ShipmentStatus getStatus() {
        return status;
    }
}