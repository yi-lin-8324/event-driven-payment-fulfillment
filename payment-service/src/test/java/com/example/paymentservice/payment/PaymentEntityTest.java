package com.example.paymentservice.payment;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PaymentEntityTest {

    @Test
    void refundChangesAuthorizedPaymentToRefunded() {
        PaymentEntity payment = new PaymentEntity(UUID.randomUUID(), new BigDecimal("42.00"));
        assertEquals(PaymentStatus.AUTHORIZED, payment.getStatus());
        payment.refund();
        assertEquals(PaymentStatus.REFUNDED, payment.getStatus());
    }
}