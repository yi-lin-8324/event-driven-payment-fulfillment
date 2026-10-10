package com.example.orderservice.order;

import com.example.inventoryservice.avro.InventoryReserved;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class InventoryReservedListener {

    private final OrderService orderService;

    public InventoryReservedListener(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(topics = "inventory-reserved")
    public void handle(ConsumerRecord<String, InventoryReserved> record) {
        InventoryReserved event = record.value();
        UUID orderId = UUID.fromString(event.getOrderId().toString());
        OrderEntity order = orderService.markInventoryReserved(orderId);
        System.out.printf(
                "Order saga updated: orderId=%s, status=%s, partition=%d, offset=%d%n",
                order.getOrderId(),
                order.getStatus(),
                record.partition(),
                record.offset()
        );
    }
}