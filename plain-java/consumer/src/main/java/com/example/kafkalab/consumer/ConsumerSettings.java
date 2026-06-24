package com.example.kafkalab.consumer;

public record ConsumerSettings(
    String bootstrapServers,
    String topic,
    String groupId,
    long processingDelayMs,
    double failureRate
) {
    public ConsumerSettings {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("bootstrapServers must not be blank");
        }
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("groupId must not be blank");
        }
        if (processingDelayMs < 0) {
            throw new IllegalArgumentException("processingDelayMs must be >= 0");
        }
        if (failureRate < 0 || failureRate > 1) {
            throw new IllegalArgumentException("failureRate must be between 0 and 1");
        }
    }
}
