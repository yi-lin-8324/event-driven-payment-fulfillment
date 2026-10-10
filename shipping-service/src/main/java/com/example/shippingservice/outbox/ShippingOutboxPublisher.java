package com.example.shippingservice.outbox;

import com.example.shippingservice.avro.ShipmentCreated;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ShippingOutboxPublisher {

    private static final String TOPIC = "shipment-created";

    private final ShippingOutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, ShipmentCreated> kafkaTemplate;

    public ShippingOutboxPublisher(
            ShippingOutboxEventRepository outboxEventRepository,
            KafkaTemplate<String, ShipmentCreated> kafkaTemplate
    ) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 1000)
    public void publishPendingEvents() {
        List<ShippingOutboxEvent> events = outboxEventRepository.findByPublishedFalseOrderByCreatedAtAsc();
        for (ShippingOutboxEvent outboxEvent : events) {
            ShipmentCreated event = new ShipmentCreated(
                    outboxEvent.getEventId().toString(),
                    outboxEvent.getOrderId().toString(),
                    outboxEvent.getCorrelationId().toString(),
                    outboxEvent.getEventType(),
                    outboxEvent.getCreatedAt(),
                    outboxEvent.getShipmentId().toString()
            );
            kafkaTemplate.send(TOPIC, outboxEvent.getOrderId().toString(), event).join();
            outboxEvent.markPublished();
            outboxEventRepository.save(outboxEvent);
        }
    }
}