package com.example.kafkalab.common.model;

import java.util.Map;

public record TopicDefinition(
    String name,
    int partitions,
    short replicationFactor,
    Map<String, String> configuration
) {
    public TopicDefinition {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("topic name must not be blank");
        }
        if (partitions < 1) {
            throw new IllegalArgumentException("partitions must be >= 1");
        }
        if (replicationFactor < 1) {
            throw new IllegalArgumentException("replicationFactor must be >= 1");
        }
        if (configuration == null) {
            configuration = Map.of();
        }
    }
}
