package com.example.paymentservice.payment;

import com.example.paymentservice.idempotency.ProcessedEvent;
import com.example.paymentservice.idempotency.ProcessedEventRepository;
import com.example.paymentservice.outbox.PaymentOutboxEvent;
import com.example.paymentservice.outbox.PaymentOutboxEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.example.paymentservice.outbox.PaymentRefundOutboxEvent;
import com.example.paymentservice.outbox.PaymentRefundOutboxEventRepository;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

@Service
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final PaymentOutboxEventRepository outboxEventRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final PaymentRefundOutboxEventRepository refundOutboxEventRepository;

    public PaymentService(
            PaymentRepository paymentRepository,
            PaymentOutboxEventRepository outboxEventRepository,
            ProcessedEventRepository processedEventRepository,
            PaymentRefundOutboxEventRepository refundOutboxEventRepository
    ) {
        this.paymentRepository = paymentRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.processedEventRepository = processedEventRepository;
        this.refundOutboxEventRepository = refundOutboxEventRepository;
    }

    @Transactional
    public Optional<PaymentEntity> authorize(UUID eventId, UUID orderId, UUID correlationId, BigDecimal amount, String eventType) {
        if (processedEventRepository.existsById(eventId)) {
            return Optional.empty();
        }
        PaymentEntity payment = new PaymentEntity(orderId, amount);
        paymentRepository.save(payment);
        PaymentOutboxEvent outboxEvent = new PaymentOutboxEvent(
                orderId,
                payment.getPaymentId(),
                correlationId,
                amount
        );
        outboxEventRepository.save(outboxEvent);
        ProcessedEvent processedEvent = new ProcessedEvent(eventId, eventType);
        processedEventRepository.save(processedEvent);
        return Optional.of(payment);
    }

    @Transactional
    public Optional<PaymentEntity> refund(UUID eventId, UUID orderId, UUID paymentId, UUID correlationId, String eventType
    ) {
        if (processedEventRepository.existsById(eventId)) {return Optional.empty();}
        PaymentEntity payment = paymentRepository.findById(paymentId).orElseThrow(() -> new IllegalStateException("Payment not found: " + paymentId));
        payment.refund();
        paymentRepository.save(payment);
        PaymentRefundOutboxEvent refundOutboxEvent = new PaymentRefundOutboxEvent(orderId, paymentId, correlationId);
        refundOutboxEventRepository.save(refundOutboxEvent);
        ProcessedEvent processedEvent = new ProcessedEvent(eventId, eventType);
        processedEventRepository.save(processedEvent);
        return Optional.of(payment);
    }
}