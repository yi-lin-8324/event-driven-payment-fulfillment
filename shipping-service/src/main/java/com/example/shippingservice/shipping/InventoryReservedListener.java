package com.example.shippingservice.shipping;

import com.example.inventoryservice.avro.InventoryReserved;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.UUID;

@Component
public class InventoryReservedListener {

    private final ShippingService shippingService;

    public InventoryReservedListener(ShippingService shippingService) {
        this.shippingService = shippingService;
    }

    @KafkaListener(topics = "inventory-reserved")
    public void handle(ConsumerRecord<String, InventoryReserved> record) {
        InventoryReserved event = record.value();
        UUID eventId = UUID.fromString(event.getEventId().toString());
        UUID orderId = UUID.fromString(event.getOrderId().toString());
        UUID correlationId = UUID.fromString(event.getCorrelationId().toString());
        Optional<ShipmentEntity> shipment = shippingService.createShipment(
                eventId,
                orderId,
                correlationId,
                event.getEventType().toString()
        );
        if (shipment.isEmpty()) {
            System.out.printf("Duplicate event skipped: eventId=%s, orderId=%s%n", eventId, orderId);
            return;
        }
        ShipmentEntity created = shipment.get();
        System.out.printf("Created shipment: shipmentId=%s, orderId=%s, partition=%d, offset=%d%n",
                created.getShipmentId(),
                created.getOrderId(),
                record.partition(),
                record.offset()
        );
    }
}