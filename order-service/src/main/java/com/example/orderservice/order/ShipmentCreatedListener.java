package com.example.orderservice.order;

import com.example.shippingservice.avro.ShipmentCreated;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class ShipmentCreatedListener {

    private final OrderService orderService;

    public ShipmentCreatedListener(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(topics = "shipment-created")
    public void handle(ConsumerRecord<String, ShipmentCreated> record) {
        ShipmentCreated event = record.value();
        UUID orderId = UUID.fromString(event.getOrderId().toString());
        OrderEntity order = orderService.markCompleted(orderId);
        System.out.printf(
                "Order saga updated: orderId=%s, status=%s, partition=%d, offset=%d%n",
                order.getOrderId(),
                order.getStatus(),
                record.partition(),
                record.offset()
        );
    }
}