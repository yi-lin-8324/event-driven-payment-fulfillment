package com.example.paymentservice.payment;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.util.UUID;

@Entity
@Table(name = "payments")
public class PaymentEntity {

    @Id
    private UUID paymentId;

    private UUID orderId;

    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    private PaymentStatus status;

    protected PaymentEntity() {}

    public PaymentEntity(UUID orderId, BigDecimal amount) {
        this.paymentId = UUID.randomUUID();
        this.orderId = orderId;
        this.amount = amount;
        this.status = PaymentStatus.AUTHORIZED;
    }

    public UUID getPaymentId() {
        return paymentId;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public PaymentStatus getStatus() {
        return status;
    }

    public void refund() {
        if (status == PaymentStatus.AUTHORIZED) {
            this.status = PaymentStatus.REFUNDED;
        }
    }
}