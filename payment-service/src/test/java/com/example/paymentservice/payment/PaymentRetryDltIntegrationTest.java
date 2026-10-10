package com.example.paymentservice.payment;

import com.example.orderservice.avro.OrderCreated;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafkaRetryTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@Testcontainers
@SpringBootTest(
        classes = PaymentRetryDltIntegrationTest.TestApplication.class,
        properties = {
                "spring.kafka.consumer.group-id=payment-retry-test",
                "spring.kafka.consumer.auto-offset-reset=earliest",
                "spring.kafka.consumer.key-deserializer="
                        + "org.apache.kafka.common.serialization.StringDeserializer",
                "spring.kafka.consumer.value-deserializer="
                        + "io.confluent.kafka.serializers.KafkaAvroDeserializer",
                "spring.kafka.consumer.properties.specific.avro.reader=true",
                "spring.kafka.consumer.properties.schema.registry.url="
                        + "mock://payment-retry-test",
                "spring.kafka.producer.key-serializer="
                        + "org.apache.kafka.common.serialization.StringSerializer",
                "spring.kafka.producer.value-serializer="
                        + "io.confluent.kafka.serializers.KafkaAvroSerializer",
                "spring.kafka.producer.properties.schema.registry.url="
                        + "mock://payment-retry-test",
                "spring.autoconfigure.exclude="
                        + "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration,"
                        + "org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration,"
                        + "org.springframework.boot.data.jpa.autoconfigure.DataJpaRepositoriesAutoConfiguration"
        }
)
class PaymentRetryDltIntegrationTest {

    @Container
    static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer("confluentinc/cp-kafka:8.3.2");

    @DynamicPropertySource
    static void configureKafka(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @MockitoBean
    private PaymentService paymentService;
    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Test
    void failedOrderCreatedIsRetriedAndSentToDlt() {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        when(paymentService.authorize(
                any(UUID.class),
                any(UUID.class),
                any(UUID.class),
                any(BigDecimal.class),
                eq("OrderCreated"))
        ).thenThrow(new IllegalStateException("simulated payment failure"));
        OrderCreated event = OrderCreated.newBuilder()
                .setEventId(eventId.toString())
                .setOrderId(orderId.toString())
                .setCorrelationId(correlationId.toString())
                .setEventType("OrderCreated")
                .setTimestamp(Instant.now())
                .setAmount("42.00")
                .build();

        kafkaTemplate.send("order-created", orderId.toString(), event).join();
        ConsumerRecord<String, OrderCreated> dltRecord = readFromDlt();
        assertNotNull(dltRecord);
        assertEquals(orderId.toString(), dltRecord.key());
        assertEquals(eventId.toString(), dltRecord.value().getEventId().toString());
        verify(paymentService, timeout(15_000).times(3)).authorize(
                eq(eventId),
                eq(orderId),
                eq(correlationId),
                eq(new BigDecimal("42.00")),
                eq("OrderCreated")
        );
    }

    @Test
    void orderCreatedIsConsumedAndPassedToPaymentService() {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();

        PaymentEntity payment = new PaymentEntity(orderId, new BigDecimal("42.00"));
        when(paymentService.authorize(
                eventId,
                orderId,
                correlationId,
                new BigDecimal("42.00"),
                "OrderCreated")
        ).thenReturn(Optional.of(payment));

        OrderCreated event = OrderCreated.newBuilder()
                .setEventId(eventId.toString())
                .setOrderId(orderId.toString())
                .setCorrelationId(correlationId.toString())
                .setEventType("OrderCreated")
                .setTimestamp(Instant.now())
                .setAmount("42.00")
                .build();

        kafkaTemplate.send("order-created", orderId.toString(), event).join();
        verify(paymentService, timeout(10000).times(1)).authorize(
                eq(eventId),
                eq(orderId),
                eq(correlationId),
                eq(new BigDecimal("42.00")),
                eq("OrderCreated")
        );
    }


    private ConsumerRecord<String, OrderCreated> readFromDlt() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-verifier-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put("schema.registry.url", "mock://payment-retry-test");
        properties.put("specific.avro.reader", true);

        try (KafkaConsumer<String, OrderCreated> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of("order-created-dlt"));
            long deadline = System.currentTimeMillis() + 15000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, OrderCreated> records = consumer.poll(Duration.ofMillis(500));
                if (!records.isEmpty()) {
                    return records.iterator().next();
                }
            }
            return null;
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EnableKafkaRetryTopic
    @Import(OrderCreatedListener.class)
    static class TestApplication {

        @Bean
        ThreadPoolTaskScheduler taskScheduler() {
            ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
            scheduler.setPoolSize(1);
            scheduler.setThreadNamePrefix("retry-test-");
            return scheduler;
        }
    }
}