package com.example.kafkalab.transactional;

import com.example.kafkalab.common.config.KafkaDefaults;

public record TransactionalProcessorSettings(
    String bootstrapServers,
    String inputTopic,
    String completedTopic,
    String failedTopic,
    String groupId,
    String transactionalId
) {
    public static final String DEFAULT_COMPLETED_TOPIC = "orders.completed.v1";
    public static final String DEFAULT_FAILED_TOPIC = "orders.failed.v1";
    public static final String DEFAULT_GROUP_ID = "order-transactional-processor";
    public static final String DEFAULT_TRANSACTIONAL_ID = "order-transactional-processor";

    public TransactionalProcessorSettings {
        requireNotBlank(bootstrapServers, "bootstrapServers");
        requireNotBlank(inputTopic, "inputTopic");
        requireNotBlank(completedTopic, "completedTopic");
        requireNotBlank(failedTopic, "failedTopic");
        requireNotBlank(groupId, "groupId");
        requireNotBlank(transactionalId, "transactionalId");
    }

    public static TransactionalProcessorSettings defaults() {
        return new TransactionalProcessorSettings(
            KafkaDefaults.DEFAULT_BOOTSTRAP_SERVERS,
            KafkaDefaults.ORDER_TOPIC,
            DEFAULT_COMPLETED_TOPIC,
            DEFAULT_FAILED_TOPIC,
            DEFAULT_GROUP_ID,
            DEFAULT_TRANSACTIONAL_ID
        );
    }

    private static void requireNotBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
