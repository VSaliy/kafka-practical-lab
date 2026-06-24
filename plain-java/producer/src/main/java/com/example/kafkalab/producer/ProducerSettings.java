package com.example.kafkalab.producer;

public record ProducerSettings(
    String bootstrapServers,
    String topic,
    int count,
    int customerCount
) {
    public ProducerSettings {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("bootstrapServers must not be blank");
        }
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        if (count < 1) {
            throw new IllegalArgumentException("count must be >= 1");
        }
        if (customerCount < 1) {
            throw new IllegalArgumentException("customerCount must be >= 1");
        }
    }
}
