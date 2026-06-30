package com.example.kafkalab.transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record OrderCompleted(
    UUID eventId,
    String orderId,
    String customerId,
    BigDecimal amount,
    String currency,
    Instant completedAt
) {
    public OrderCompleted {
        Objects.requireNonNull(eventId, "eventId must not be null");
        requireNotBlank(orderId, "orderId");
        requireNotBlank(customerId, "customerId");
        Objects.requireNonNull(amount, "amount must not be null");
        requireNotBlank(currency, "currency");
        Objects.requireNonNull(completedAt, "completedAt must not be null");
    }

    private static void requireNotBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
