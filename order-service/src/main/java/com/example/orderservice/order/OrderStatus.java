package com.example.orderservice.order;

public enum OrderStatus {
    CREATED,
    PAYMENT_AUTHORIZED,
    INVENTORY_RESERVED,
    COMPLETED,
    CANCELLED
}