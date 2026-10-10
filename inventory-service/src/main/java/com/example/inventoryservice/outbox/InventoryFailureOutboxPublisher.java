package com.example.inventoryservice.outbox;

import com.example.inventoryservice.avro.InventoryReservationFailed;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class InventoryFailureOutboxPublisher {

    private final InventoryFailureOutboxEventRepository repository;
    private final KafkaTemplate<String, InventoryReservationFailed> kafkaTemplate;

    public InventoryFailureOutboxPublisher(
            InventoryFailureOutboxEventRepository repository,
            KafkaTemplate<String, InventoryReservationFailed> kafkaTemplate
    ) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 1000)
    public void publish() {
        for (InventoryFailureOutboxEvent outboxEvent : repository.findByPublishedFalse()) {
            InventoryReservationFailed event = new InventoryReservationFailed();
            event.setEventId(outboxEvent.getEventId().toString());
            event.setOrderId(outboxEvent.getOrderId().toString());
            event.setCorrelationId(outboxEvent.getCorrelationId().toString());
            event.setEventType(outboxEvent.getEventType());
            event.setTimestamp(outboxEvent.getCreatedAt().toEpochMilli());
            event.setPaymentId(outboxEvent.getPaymentId().toString());
            event.setReason(outboxEvent.getReason());
            kafkaTemplate.send("inventory-reservation-failed", outboxEvent.getOrderId().toString(), event).join();
            outboxEvent.markPublished();
            repository.save(outboxEvent);
            System.out.printf("Published InventoryReservationFailed: eventId=%s, orderId=%s, paymentId=%s%n",
                    outboxEvent.getEventId(),
                    outboxEvent.getOrderId(),
                    outboxEvent.getPaymentId()
            );
        }
    }
}