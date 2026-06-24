package com.example.kafkalab.common.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class OrderCreatedTest {

    private static final UUID EVENT_ID = UUID.randomUUID();
    private static final String ORDER_ID = "order-001";
    private static final String CUSTOMER_ID = "customer-001";
    private static final BigDecimal AMOUNT = new BigDecimal("99.99");
    private static final String CURRENCY = "USD";
    private static final Instant NOW = Instant.now();

    @Test
    void shouldCreateValidOrderCreated() {
        OrderCreated order = new OrderCreated(EVENT_ID, ORDER_ID, CUSTOMER_ID, AMOUNT, CURRENCY, NOW);
        assertThat(order.eventId()).isEqualTo(EVENT_ID);
        assertThat(order.orderId()).isEqualTo(ORDER_ID);
        assertThat(order.customerId()).isEqualTo(CUSTOMER_ID);
        assertThat(order.amount()).isEqualTo(AMOUNT);
        assertThat(order.currency()).isEqualTo(CURRENCY);
        assertThat(order.createdAt()).isEqualTo(NOW);
    }

    @Test
    void shouldRejectNullEventId() {
        assertThatNullPointerException()
            .isThrownBy(() -> new OrderCreated(null, ORDER_ID, CUSTOMER_ID, AMOUNT, CURRENCY, NOW))
            .withMessageContaining("eventId");
    }

    @Test
    void shouldRejectNullOrderId() {
        assertThatNullPointerException()
            .isThrownBy(() -> new OrderCreated(EVENT_ID, null, CUSTOMER_ID, AMOUNT, CURRENCY, NOW))
            .withMessageContaining("orderId");
    }

    @Test
    void shouldRejectBlankOrderId() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> new OrderCreated(EVENT_ID, "  ", CUSTOMER_ID, AMOUNT, CURRENCY, NOW))
            .withMessageContaining("orderId");
    }

    @Test
    void shouldRejectNullCustomerId() {
        assertThatNullPointerException()
            .isThrownBy(() -> new OrderCreated(EVENT_ID, ORDER_ID, null, AMOUNT, CURRENCY, NOW))
            .withMessageContaining("customerId");
    }

    @Test
    void shouldRejectNullAmount() {
        assertThatNullPointerException()
            .isThrownBy(() -> new OrderCreated(EVENT_ID, ORDER_ID, CUSTOMER_ID, null, CURRENCY, NOW))
            .withMessageContaining("amount");
    }

    @Test
    void shouldRejectZeroAmount() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> new OrderCreated(EVENT_ID, ORDER_ID, CUSTOMER_ID, BigDecimal.ZERO, CURRENCY, NOW))
            .withMessageContaining("amount");
    }

    @Test
    void shouldRejectNegativeAmount() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> new OrderCreated(EVENT_ID, ORDER_ID, CUSTOMER_ID, new BigDecimal("-1"), CURRENCY, NOW))
            .withMessageContaining("amount");
    }

    @Test
    void shouldRejectInvalidCurrency() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> new OrderCreated(EVENT_ID, ORDER_ID, CUSTOMER_ID, AMOUNT, "usd", NOW))
            .withMessageContaining("currency");
    }

    @Test
    void shouldRejectNullCreatedAt() {
        assertThatNullPointerException()
            .isThrownBy(() -> new OrderCreated(EVENT_ID, ORDER_ID, CUSTOMER_ID, AMOUNT, CURRENCY, null))
            .withMessageContaining("createdAt");
    }
}
