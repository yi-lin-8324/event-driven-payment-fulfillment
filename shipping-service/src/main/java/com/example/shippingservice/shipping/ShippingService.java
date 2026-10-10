package com.example.shippingservice.shipping;

import com.example.shippingservice.idempotency.ProcessedEvent;
import com.example.shippingservice.idempotency.ProcessedEventRepository;
import com.example.shippingservice.outbox.ShippingOutboxEvent;
import com.example.shippingservice.outbox.ShippingOutboxEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Service
public class ShippingService {

    private final ShipmentRepository shipmentRepository;
    private final ShippingOutboxEventRepository outboxEventRepository;
    private final ProcessedEventRepository processedEventRepository;

    public ShippingService(
            ShipmentRepository shipmentRepository,
            ShippingOutboxEventRepository outboxEventRepository,
            ProcessedEventRepository processedEventRepository
    ) {
        this.shipmentRepository = shipmentRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional
    public Optional<ShipmentEntity> createShipment(UUID eventId, UUID orderId, UUID correlationId, String eventType) {
        if (processedEventRepository.existsById(eventId)) {return Optional.empty();}
        ShipmentEntity shipment = new ShipmentEntity(orderId);
        shipmentRepository.save(shipment);
        ShippingOutboxEvent outboxEvent = new ShippingOutboxEvent(
                        orderId,
                        shipment.getShipmentId(),
                        correlationId
                );
        outboxEventRepository.save(outboxEvent);
        ProcessedEvent processedEvent = new ProcessedEvent(eventId, eventType);
        processedEventRepository.save(processedEvent);
        return Optional.of(shipment);
    }
}