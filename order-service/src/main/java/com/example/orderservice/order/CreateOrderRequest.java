package com.example.orderservice.order;

import java.math.BigDecimal;

public record CreateOrderRequest(BigDecimal amount) {
}