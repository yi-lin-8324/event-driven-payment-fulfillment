package com.example.orderservice.order;

import com.example.paymentservice.avro.PaymentRefunded;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class PaymentRefundedListener {

    private final OrderService orderService;

    public PaymentRefundedListener(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(topics = "payment-refunded")
    public void handle(ConsumerRecord<String, PaymentRefunded> record) {
        PaymentRefunded event = record.value();
        UUID orderId = UUID.fromString(event.getOrderId().toString());
        orderService.markCancelled(orderId);
        System.out.printf("Order cancelled after payment refund: orderId=%s, paymentId=%s, partition=%d, offset=%d%n", orderId, event.getPaymentId(), record.partition(), record.offset());
    }
}