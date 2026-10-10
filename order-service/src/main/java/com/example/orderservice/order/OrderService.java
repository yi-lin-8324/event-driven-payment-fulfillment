package com.example.orderservice.order;

import com.example.orderservice.outbox.OutboxEvent;
import com.example.orderservice.outbox.OutboxEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;

    public OrderService(
            OrderRepository orderRepository,
            OutboxEventRepository outboxEventRepository
    ) {
        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
    }

    @Transactional
    public OrderEntity createOrder(BigDecimal amount) {
        OrderEntity order = new OrderEntity(amount);
        orderRepository.save(order);
        UUID correlationId = UUID.randomUUID();
        OutboxEvent outboxEvent = new OutboxEvent(order.getOrderId(), correlationId, "OrderCreated", order.getAmount());
        outboxEventRepository.save(outboxEvent);
        return order;
    }

    @Transactional
    public OrderEntity markPaymentAuthorized(UUID orderId) {
        OrderEntity order = orderRepository.findById(orderId).orElseThrow(() -> new IllegalArgumentException("Order not found: " + orderId));
        order.markPaymentAuthorized();
        return orderRepository.save(order);
    }

    @Transactional
    public OrderEntity markInventoryReserved(UUID orderId) {
        OrderEntity order = orderRepository.findById(orderId).orElseThrow(() -> new IllegalArgumentException("Order not found: " + orderId));
        order.markInventoryReserved();
        return orderRepository.save(order);
    }

    @Transactional
    public OrderEntity markCompleted(UUID orderId) {
        OrderEntity order = orderRepository.findById(orderId).orElseThrow(() -> new IllegalArgumentException("Order not found: " + orderId));
        order.markCompleted();
        return orderRepository.save(order);
    }

    @Transactional
    public void markCancelled(UUID orderId) {
        OrderEntity order = orderRepository.findById(orderId).orElseThrow(() -> new IllegalStateException("Order not found: " + orderId));
        order.markCancelled();
        orderRepository.save(order);
    }
}