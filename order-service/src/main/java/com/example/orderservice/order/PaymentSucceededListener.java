package com.example.orderservice.order;

import com.example.paymentservice.avro.PaymentSucceeded;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class PaymentSucceededListener {

    private final OrderService orderService;

    public PaymentSucceededListener(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(topics = "payment-succeeded")
    public void handle(ConsumerRecord<String, PaymentSucceeded> record) {
        PaymentSucceeded event = record.value();
        UUID orderId = UUID.fromString(event.getOrderId().toString());
        OrderEntity order = orderService.markPaymentAuthorized(orderId);
        System.out.printf(
                "Order saga updated: orderId=%s, status=%s, partition=%d, offset=%d%n",
                order.getOrderId(),
                order.getStatus(),
                record.partition(),
                record.offset()
        );
    }
}