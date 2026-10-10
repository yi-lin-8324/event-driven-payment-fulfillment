package com.example.paymentservice.payment;

import com.example.paymentservice.idempotency.ProcessedEvent;
import com.example.paymentservice.idempotency.ProcessedEventRepository;
import com.example.paymentservice.outbox.PaymentOutboxEventRepository;
import com.example.paymentservice.outbox.PaymentRefundOutboxEvent;
import com.example.paymentservice.outbox.PaymentRefundOutboxEventRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock
    private PaymentRepository paymentRepository;

    @Mock
    private PaymentOutboxEventRepository outboxEventRepository;

    @Mock
    private ProcessedEventRepository processedEventRepository;

    @Mock
    private PaymentRefundOutboxEventRepository refundOutboxEventRepository;

    @Test
    void refundCreatesCompensationAndMarksEventProcessed() {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        PaymentEntity payment = new PaymentEntity(orderId, new BigDecimal("42.00"));
        UUID paymentId = payment.getPaymentId();
        when(processedEventRepository.existsById(eventId)).thenReturn(false);
        when(paymentRepository.findById(paymentId)).thenReturn(Optional.of(payment));
        PaymentService paymentService = new PaymentService(
                paymentRepository,
                outboxEventRepository,
                processedEventRepository,
                refundOutboxEventRepository
        );
        Optional<PaymentEntity> result = paymentService.refund(eventId, orderId, paymentId, correlationId, "InventoryReservationFailed");
        assertTrue(result.isPresent());
        assertEquals(PaymentStatus.REFUNDED, result.get().getStatus());
        verify(paymentRepository).save(payment);
        ArgumentCaptor<PaymentRefundOutboxEvent> outboxCaptor = ArgumentCaptor.forClass(PaymentRefundOutboxEvent.class);
        verify(refundOutboxEventRepository).save(outboxCaptor.capture());
        PaymentRefundOutboxEvent outboxEvent = outboxCaptor.getValue();
        assertEquals(orderId, outboxEvent.getOrderId());
        assertEquals(paymentId, outboxEvent.getPaymentId());
        assertEquals(correlationId, outboxEvent.getCorrelationId());
        verify(processedEventRepository).save(any(ProcessedEvent.class));
    }

    @Test
    void duplicateRefundEventIsSkipped() {
        UUID eventId = UUID.randomUUID();
        when(processedEventRepository.existsById(eventId)).thenReturn(true);
        PaymentService paymentService = new PaymentService(
                paymentRepository,
                outboxEventRepository,
                processedEventRepository,
                refundOutboxEventRepository
        );
        Optional<PaymentEntity> result = paymentService.refund(
                eventId,
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                "InventoryReservationFailed"
        );
        assertTrue(result.isEmpty());
        verifyNoInteractions(paymentRepository);
        verifyNoInteractions(refundOutboxEventRepository);
        verifyNoInteractions(outboxEventRepository);
        verify(processedEventRepository, never()).save(any());
    }
}