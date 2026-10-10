package com.example.orderservice.outbox;

import com.example.orderservice.avro.OrderCreated;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class OutboxPublisher {

    private static final String TOPIC = "order-created";

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, OrderCreated> kafkaTemplate;

    public OutboxPublisher(
            OutboxEventRepository outboxEventRepository,
            KafkaTemplate<String, OrderCreated> kafkaTemplate
    ) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 1000)
    public void publishPendingEvents() {
        List<OutboxEvent> events = outboxEventRepository.findByPublishedFalseOrderByCreatedAtAsc();

        for (OutboxEvent outboxEvent : events) {
            OrderCreated event = new OrderCreated(
                    outboxEvent.getEventId().toString(),
                    outboxEvent.getOrderId().toString(),
                    outboxEvent.getCorrelationId().toString(),
                    outboxEvent.getEventType(),
                    outboxEvent.getCreatedAt(),
                    outboxEvent.getAmount().toPlainString(),
                    "order-service"
            );

            kafkaTemplate.send(TOPIC, outboxEvent.getOrderId().toString(), event).join();
            outboxEvent.markPublished();
            outboxEventRepository.save(outboxEvent);
        }
    }
}