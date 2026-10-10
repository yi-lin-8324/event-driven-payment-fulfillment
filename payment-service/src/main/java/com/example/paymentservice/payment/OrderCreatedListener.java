package com.example.paymentservice.payment;

import com.example.orderservice.avro.OrderCreated;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

@Component
public class OrderCreatedListener {

    private final PaymentService paymentService;

    public OrderCreatedListener(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @RetryableTopic(
            attempts = "3",
            backOff = @BackOff(delay = 1000),
            numPartitions = "3",
            replicationFactor = "1",
            kafkaTemplate = "kafkaTemplate",
            sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS
    )
    @KafkaListener(topics = "order-created")
    public void handle(ConsumerRecord<String, OrderCreated> record) {
        OrderCreated event = record.value();

        UUID eventId = UUID.fromString(event.getEventId().toString());
        UUID orderId = UUID.fromString(event.getOrderId().toString());
        UUID correlationId = UUID.fromString(event.getCorrelationId().toString());
        BigDecimal amount = new BigDecimal(event.getAmount().toString());

        Optional<PaymentEntity> payment = paymentService.authorize(
                eventId,
                orderId,
                correlationId,
                amount,
                event.getEventType().toString()
        );

        if (payment.isEmpty()) {
            System.out.printf("Duplicate event skipped: eventId=%s, orderId=%s%n", eventId, orderId);
            return;
        }

        PaymentEntity created = payment.get();
        System.out.printf("Payment authorized: paymentId=%s, orderId=%s, partition=%d, offset=%d%n",
                created.getPaymentId(),
                created.getOrderId(),
                record.partition(),
                record.offset()
        );
    }
}