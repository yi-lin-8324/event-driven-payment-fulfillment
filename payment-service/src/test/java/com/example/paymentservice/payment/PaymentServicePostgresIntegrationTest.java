package com.example.paymentservice.payment;

import com.example.paymentservice.idempotency.ProcessedEventRepository;
import com.example.paymentservice.outbox.PaymentOutboxEventRepository;
import com.example.paymentservice.outbox.PaymentRefundOutboxEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@DataJpaTest(properties = {"spring.jpa.hibernate.ddl-auto=create-drop"})
@ContextConfiguration(classes = PaymentServicePostgresIntegrationTest.JpaTestConfiguration.class)
@Import(PaymentService.class)
class PaymentServicePostgresIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17.11")
            .withDatabaseName("payment_test")
            .withUsername("postgres")
            .withPassword("postgres");

    @DynamicPropertySource
    static void configurePostgres(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    private PaymentService paymentService;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private PaymentOutboxEventRepository outboxEventRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private PaymentRefundOutboxEventRepository refundOutboxEventRepository;

    @Test
    void authorizePersistsPaymentOutboxAndProcessedEvent() {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();

        Optional<PaymentEntity> result =
                paymentService.authorize(
                        eventId,
                        orderId,
                        correlationId,
                        new BigDecimal("42.00"),
                        "OrderCreated"
                );

        assertTrue(result.isPresent());
        UUID paymentId = result.get().getPaymentId();
        assertTrue(paymentRepository.findById(paymentId).isPresent());
        assertEquals(1, outboxEventRepository.count());
        assertTrue(processedEventRepository.existsById(eventId));
        assertEquals(0, refundOutboxEventRepository.count());
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void authorizeRollsBackEverythingWhenProcessedEventInsertFails() {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        String invalidEventType = "X".repeat(300);
        assertThrows(DataIntegrityViolationException.class, () -> paymentService.authorize(
                        eventId,
                        orderId,
                        correlationId,
                        new BigDecimal("42.00"),
                        invalidEventType
                )
        );
        assertEquals(0, paymentRepository.count());
        assertEquals(0, outboxEventRepository.count());
        assertEquals(0, processedEventRepository.count());
    }

    @Test
    void refundPersistsRefundedPaymentAndCompensationOutbox() {
        UUID orderCreatedEventId = UUID.randomUUID();
        UUID inventoryFailedEventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();

        PaymentEntity payment = paymentService.authorize(
                orderCreatedEventId,
                orderId,
                correlationId,
                new BigDecimal("42.00"),
                "OrderCreated"
        ).orElseThrow();

        UUID paymentId = payment.getPaymentId();
        PaymentEntity refundedPayment = paymentService.refund(
                inventoryFailedEventId,
                orderId,
                paymentId,
                correlationId,
                "InventoryReservationFailed"
        ).orElseThrow();
        assertEquals(PaymentStatus.REFUNDED, refundedPayment.getStatus());
        PaymentEntity storedPayment = paymentRepository.findById(paymentId).orElseThrow();
        assertEquals(PaymentStatus.REFUNDED, storedPayment.getStatus());
        assertEquals(1, refundOutboxEventRepository.count());
        assertTrue(processedEventRepository.existsById(inventoryFailedEventId));
    }

    @Test
    void duplicateAuthorizeEventDoesNotCreateDuplicatePaymentOrOutbox() {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();

        Optional<PaymentEntity> firstResult = paymentService.authorize(
                eventId,
                orderId,
                correlationId,
                new BigDecimal("42.00"),
                "OrderCreated"
        );

        Optional<PaymentEntity> secondResult = paymentService.authorize(
                eventId,
                orderId,
                correlationId,
                new BigDecimal("42.00"),
                "OrderCreated"
        );

        assertTrue(firstResult.isPresent());
        assertTrue(secondResult.isEmpty());
        assertEquals(1, paymentRepository.count());
        assertEquals(1, outboxEventRepository.count());
        assertEquals(1, processedEventRepository.count());
    }

    @Configuration
    @EntityScan("com.example.paymentservice")
    @EnableJpaRepositories("com.example.paymentservice")
    static class JpaTestConfiguration {
    }
}