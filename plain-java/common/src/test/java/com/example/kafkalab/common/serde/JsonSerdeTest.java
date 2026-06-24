package com.example.kafkalab.common.serde;

import com.example.kafkalab.common.model.OrderCreated;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatRuntimeException;

class JsonSerdeTest {

    @Test
    void shouldSerializeAndDeserializeOrderCreated() {
        UUID eventId = UUID.randomUUID();
        Instant now = Instant.now();
        OrderCreated original = new OrderCreated(eventId, "order-1", "cust-1", new BigDecimal("50.00"), "EUR", now);

        String json = JsonSerde.toJson(original);
        assertThat(json).isNotBlank();
        assertThat(json).contains("order-1");

        OrderCreated deserialized = JsonSerde.fromJson(json, OrderCreated.class);
        assertThat(deserialized.orderId()).isEqualTo("order-1");
        assertThat(deserialized.eventId()).isEqualTo(eventId);
        assertThat(deserialized.amount()).isEqualByComparingTo(new BigDecimal("50.00"));
    }

    @Test
    void shouldReturnMapper() {
        assertThat(JsonSerde.mapper()).isNotNull();
    }

    @Test
    void shouldThrowOnInvalidJson() {
        assertThatRuntimeException()
            .isThrownBy(() -> JsonSerde.fromJson("{invalid}", OrderCreated.class));
    }
}
