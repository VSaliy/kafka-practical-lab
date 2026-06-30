package com.example.kafkalab.transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record OrderFailed(
    UUID eventId,
    String orderId,
    String customerId,
    BigDecimal amount,
    String currency,
    String reason,
    Instant failedAt
) {
    public OrderFailed {
        Objects.requireNonNull(eventId, "eventId must not be null");
        requireNotBlank(orderId, "orderId");
        requireNotBlank(customerId, "customerId");
        Objects.requireNonNull(amount, "amount must not be null");
        requireNotBlank(currency, "currency");
        requireNotBlank(reason, "reason");
        Objects.requireNonNull(failedAt, "failedAt must not be null");
    }

    private static void requireNotBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
