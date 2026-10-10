package com.example.paymentservice.outbox;

import com.example.paymentservice.avro.PaymentRefunded;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class PaymentRefundOutboxPublisher {

    private final PaymentRefundOutboxEventRepository repository;
    private final KafkaTemplate<String, PaymentRefunded> kafkaTemplate;

    public PaymentRefundOutboxPublisher(
            PaymentRefundOutboxEventRepository repository,
            KafkaTemplate<String, PaymentRefunded> kafkaTemplate
    ) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 1000)
    public void publish() {
        for (PaymentRefundOutboxEvent outboxEvent : repository.findByPublishedFalse()) {
            PaymentRefunded event = new PaymentRefunded();
            event.setEventId(outboxEvent.getEventId().toString());
            event.setOrderId(outboxEvent.getOrderId().toString());
            event.setCorrelationId(outboxEvent.getCorrelationId().toString());
            event.setEventType(outboxEvent.getEventType());
            event.setTimestamp(outboxEvent.getCreatedAt().toEpochMilli());
            event.setPaymentId(outboxEvent.getPaymentId().toString());
            kafkaTemplate.send("payment-refunded", outboxEvent.getOrderId().toString(), event).join();
            outboxEvent.markPublished();
            repository.save(outboxEvent);
            System.out.printf("Published PaymentRefunded: eventId=%s, orderId=%s, paymentId=%s%n", outboxEvent.getEventId(), outboxEvent.getOrderId(), outboxEvent.getPaymentId());
        }
    }
}