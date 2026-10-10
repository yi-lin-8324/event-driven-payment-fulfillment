package com.example.inventoryservice.inventory;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.util.UUID;

@Entity
@Table(name = "inventory_reservations")
public class InventoryReservation {

    @Id
    private UUID reservationId;

    private UUID orderId;

    @Enumerated(EnumType.STRING)
    private InventoryStatus status;

    protected InventoryReservation() {}

    public InventoryReservation(UUID orderId) {
        this.reservationId = UUID.randomUUID();
        this.orderId = orderId;
        this.status = InventoryStatus.RESERVED;
    }

    public UUID getReservationId() {
        return reservationId;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public InventoryStatus getStatus() {
        return status;
    }
}