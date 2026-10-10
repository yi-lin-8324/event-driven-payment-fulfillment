package com.example.inventoryservice.inventory;

import com.example.paymentservice.avro.PaymentSucceeded;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

@Component
public class PaymentSucceededListener {

    private final InventoryService inventoryService;

    public PaymentSucceededListener(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @KafkaListener(topics = "payment-succeeded")
    public void handle(ConsumerRecord<String, PaymentSucceeded> record) {
        PaymentSucceeded event = record.value();
        UUID eventId = UUID.fromString(event.getEventId().toString());
        UUID orderId = UUID.fromString(event.getOrderId().toString());
        UUID correlationId = UUID.fromString(event.getCorrelationId().toString());
        //==============(test)=================

        UUID paymentId = UUID.fromString(event.getPaymentId().toString());
        BigDecimal amount = new BigDecimal(event.getAmount().toString());
        String eventType = event.getEventType().toString();

        if (amount.compareTo(new BigDecimal("888.00")) == 0) {
            boolean processed = inventoryService.failReservation(
                    eventId,
                    orderId,
                    paymentId,
                    correlationId,
                    eventType,
                    "OUT_OF_STOCK"
            );

            if (!processed) {
                System.out.printf("Duplicate event skipped: eventId=%s, orderId=%s%n", eventId, orderId);
            } else {
                System.out.printf("Inventory reservation failed: orderId=%s, paymentId=%s, reason=OUT_OF_STOCK%n", orderId, paymentId);
            }
            return;
        }

        //==============(test)=================

        Optional<InventoryReservation> reservation =
                inventoryService.reserve(
                        eventId,
                        orderId,
                        correlationId,
                        event.getEventType().toString()
                );
        if (reservation.isEmpty()) {
            System.out.printf("Duplicate event skipped: eventId=%s, orderId=%s%n", eventId, orderId);
            return;
        }
        InventoryReservation created = reservation.get();
        System.out.printf(
                "Reserved inventory: reservationId=%s, orderId=%s, " + "partition=%d, offset=%d%n",
                created.getReservationId(),
                created.getOrderId(),
                record.partition(),
                record.offset()
        );
    }
}