package com.example.paymentservice.payment;

import com.example.inventoryservice.avro.InventoryReservationFailed;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.UUID;

@Component
public class InventoryReservationFailedListener {

    private final PaymentService paymentService;

    public InventoryReservationFailedListener(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @KafkaListener(topics = "inventory-reservation-failed")
    public void handle(ConsumerRecord<String, InventoryReservationFailed> record) {
        InventoryReservationFailed event = record.value();
        UUID eventId = UUID.fromString(event.getEventId().toString());
        UUID orderId = UUID.fromString(event.getOrderId().toString());
        UUID paymentId = UUID.fromString(event.getPaymentId().toString());
        UUID correlationId = UUID.fromString(event.getCorrelationId().toString());
        String eventType = event.getEventType().toString();
        Optional<PaymentEntity> result = paymentService.refund(eventId, orderId, paymentId, correlationId, eventType);
        if (result.isEmpty()) {
            System.out.printf("Duplicate compensation event skipped: eventId=%s, orderId=%s%n", eventId, orderId);
            return;
        }
        PaymentEntity payment = result.get();
        System.out.printf("Payment refunded: paymentId=%s, orderId=%s, status=%s, partition=%d, offset=%d%n",
                payment.getPaymentId(),
                payment.getOrderId(),
                payment.getStatus(),
                record.partition(),
                record.offset()
        );
    }
}