package com.example.orderservice.order;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class OrderEntity {

    @Id
    private UUID orderId;

    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    protected OrderEntity() {}

    public OrderEntity(BigDecimal amount) {
        this.orderId = UUID.randomUUID();
        this.amount = amount;
        this.status = OrderStatus.CREATED;
    }

    public UUID getOrderId() {return orderId;}
    public BigDecimal getAmount() {return amount;}
    public OrderStatus getStatus() {return status;}


    public void markPaymentAuthorized() {
        if (status == OrderStatus.CREATED) {
            this.status = OrderStatus.PAYMENT_AUTHORIZED;
        }
    }
    public void markInventoryReserved() {
        if (status == OrderStatus.CREATED || status == OrderStatus.PAYMENT_AUTHORIZED) {
            this.status = OrderStatus.INVENTORY_RESERVED;
        }
    }
    public void markCompleted() {
        if (status == OrderStatus.CREATED || status == OrderStatus.PAYMENT_AUTHORIZED || status == OrderStatus.INVENTORY_RESERVED) {
            this.status = OrderStatus.COMPLETED;
        }
    }

    public void markCancelled() {
        if (status == OrderStatus.CREATED || status == OrderStatus.PAYMENT_AUTHORIZED) {
            this.status = OrderStatus.CANCELLED;
        }
    }
}