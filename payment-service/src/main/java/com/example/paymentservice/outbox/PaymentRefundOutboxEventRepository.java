package com.example.paymentservice.outbox;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface PaymentRefundOutboxEventRepository extends JpaRepository<PaymentRefundOutboxEvent, UUID> {

    List<PaymentRefundOutboxEvent> findByPublishedFalse();
}