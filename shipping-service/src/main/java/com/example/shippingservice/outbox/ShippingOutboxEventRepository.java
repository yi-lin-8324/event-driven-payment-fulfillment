package com.example.shippingservice.outbox;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface ShippingOutboxEventRepository extends JpaRepository<ShippingOutboxEvent, UUID> {

    List<ShippingOutboxEvent> findByPublishedFalseOrderByCreatedAtAsc();
}