package com.example.inventoryservice.inventory;

import com.example.inventoryservice.idempotency.ProcessedEvent;
import com.example.inventoryservice.idempotency.ProcessedEventRepository;
import com.example.inventoryservice.outbox.InventoryOutboxEvent;
import com.example.inventoryservice.outbox.InventoryOutboxEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.example.inventoryservice.outbox.InventoryFailureOutboxEvent;
import com.example.inventoryservice.outbox.InventoryFailureOutboxEventRepository;

import java.util.Optional;
import java.util.UUID;

@Service
public class InventoryService {

    private final InventoryReservationRepository reservationRepository;
    private final InventoryOutboxEventRepository outboxEventRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final InventoryFailureOutboxEventRepository failureOutboxEventRepository;

    public InventoryService(
            InventoryReservationRepository reservationRepository,
            InventoryOutboxEventRepository outboxEventRepository,
            ProcessedEventRepository processedEventRepository,
            InventoryFailureOutboxEventRepository failureOutboxEventRepository
    ) {
        this.reservationRepository = reservationRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.processedEventRepository = processedEventRepository;
        this.failureOutboxEventRepository = failureOutboxEventRepository;

    }

    @Transactional
    public Optional<InventoryReservation> reserve(UUID eventId, UUID orderId, UUID correlationId, String eventType) {
        if (processedEventRepository.existsById(eventId)) {
            return Optional.empty();
        }
        InventoryReservation reservation = new InventoryReservation(orderId);
        reservationRepository.save(reservation);
        InventoryOutboxEvent outboxEvent = new InventoryOutboxEvent(orderId, reservation.getReservationId(), correlationId);
        outboxEventRepository.save(outboxEvent);
        ProcessedEvent processedEvent = new ProcessedEvent(eventId, eventType);
        processedEventRepository.save(processedEvent);
        return Optional.of(reservation);
    }

    @Transactional
    public boolean failReservation(UUID eventId, UUID orderId, UUID paymentId, UUID correlationId, String eventType, String reason) {
        if (processedEventRepository.existsById(eventId)) {
            return false;
        }
        InventoryFailureOutboxEvent failureEvent = new InventoryFailureOutboxEvent(orderId, paymentId, correlationId, reason);
        failureOutboxEventRepository.save(failureEvent);
        ProcessedEvent processedEvent = new ProcessedEvent(eventId, eventType);
        processedEventRepository.save(processedEvent);
        return true;
    }
}