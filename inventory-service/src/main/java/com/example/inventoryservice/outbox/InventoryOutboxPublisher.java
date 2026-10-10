package com.example.inventoryservice.outbox;

import com.example.inventoryservice.avro.InventoryReserved;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class InventoryOutboxPublisher {

    private static final String TOPIC = "inventory-reserved";

    private final InventoryOutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, InventoryReserved> kafkaTemplate;

    public InventoryOutboxPublisher(
            InventoryOutboxEventRepository outboxEventRepository,
            KafkaTemplate<String, InventoryReserved> kafkaTemplate
    ) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 1000)
    public void publishPendingEvents() {
        List<InventoryOutboxEvent> events = outboxEventRepository.findByPublishedFalseOrderByCreatedAtAsc();
        for (InventoryOutboxEvent outboxEvent : events) {
            InventoryReserved event = new InventoryReserved(
                    outboxEvent.getEventId().toString(),
                    outboxEvent.getOrderId().toString(),
                    outboxEvent.getCorrelationId().toString(),
                    outboxEvent.getEventType(),
                    outboxEvent.getCreatedAt(),
                    outboxEvent.getReservationId().toString()
            );
            kafkaTemplate.send(TOPIC, outboxEvent.getOrderId().toString(), event).join();
            outboxEvent.markPublished();
            outboxEventRepository.save(outboxEvent);
        }
    }
}