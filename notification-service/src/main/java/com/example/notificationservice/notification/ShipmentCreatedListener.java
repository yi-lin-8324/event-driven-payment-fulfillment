package com.example.notificationservice.notification;

import com.example.shippingservice.avro.ShipmentCreated;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ShipmentCreatedListener {

    @KafkaListener(topics = "shipment-created")
    public void handle(ConsumerRecord<String, ShipmentCreated> record) {
        ShipmentCreated event = record.value();
        System.out.printf("Notification recorded: orderId=%s, shipmentId=%s, " + "correlationId=%s, partition=%d, offset=%d%n",
                event.getOrderId(),
                event.getShipmentId(),
                event.getCorrelationId(),
                record.partition(),
                record.offset()
        );
    }
}