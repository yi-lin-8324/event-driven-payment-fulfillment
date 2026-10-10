package com.example.paymentservice.outbox;

import com.example.paymentservice.avro.PaymentSucceeded;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class PaymentOutboxPublisher {

    private static final String TOPIC = "payment-succeeded";

    private final PaymentOutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, PaymentSucceeded> kafkaTemplate;

    public PaymentOutboxPublisher(
            PaymentOutboxEventRepository outboxEventRepository,
            KafkaTemplate<String, PaymentSucceeded> kafkaTemplate
    ) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 1000)
    public void publishPendingEvents() {
        List<PaymentOutboxEvent> events = outboxEventRepository.findByPublishedFalseOrderByCreatedAtAsc();

        for (PaymentOutboxEvent outboxEvent : events) {
            PaymentSucceeded event = new PaymentSucceeded(
                    outboxEvent.getEventId().toString(),
                    outboxEvent.getOrderId().toString(),
                    outboxEvent.getCorrelationId().toString(),
                    outboxEvent.getEventType(),
                    outboxEvent.getCreatedAt(),
                    outboxEvent.getPaymentId().toString(),
                    outboxEvent.getAmount().toPlainString()
            );

            kafkaTemplate.send(TOPIC, outboxEvent.getOrderId().toString(), event).join();
            outboxEvent.markPublished();
            outboxEventRepository.save(outboxEvent);
        }
    }
}