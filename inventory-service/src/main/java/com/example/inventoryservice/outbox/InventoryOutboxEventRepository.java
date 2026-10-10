package com.example.inventoryservice.outbox;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface InventoryOutboxEventRepository extends JpaRepository<InventoryOutboxEvent, UUID> {

    List<InventoryOutboxEvent> findByPublishedFalseOrderByCreatedAtAsc();
}