package com.example.inventoryservice.outbox;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface InventoryFailureOutboxEventRepository extends JpaRepository<InventoryFailureOutboxEvent, UUID> {

    List<InventoryFailureOutboxEvent> findByPublishedFalse();
}