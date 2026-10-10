package com.example.inventoryservice.inventory;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface InventoryReservationRepository extends JpaRepository<InventoryReservation, UUID> {
}