package com.example.kafkalab.admin;

import com.example.kafkalab.common.config.KafkaDefaults;
import com.example.kafkalab.common.model.TopicDefinition;
import java.util.List;
import java.util.Map;

public final class TopicCatalog {
    private static final Map<String, String> BUSINESS_CONFIG = Map.of(
        "retention.ms", KafkaDefaults.RETENTION_MS_7_DAYS,
        "cleanup.policy", "delete"
    );
    private static final Map<String, String> DLT_CONFIG = Map.of(
        "retention.ms", "2592000000",
        "cleanup.policy", "delete"
    );

    private TopicCatalog() {
    }

    public static List<TopicDefinition> all() {
        return List.of(
            new TopicDefinition("orders.created.v1", 3, (short) 1, BUSINESS_CONFIG),
            new TopicDefinition("inventory.commands.v1", 3, (short) 1, BUSINESS_CONFIG),
            new TopicDefinition("inventory.events.v1", 3, (short) 1, BUSINESS_CONFIG),
            new TopicDefinition("payments.commands.v1", 3, (short) 1, BUSINESS_CONFIG),
            new TopicDefinition("payments.events.v1", 3, (short) 1, BUSINESS_CONFIG),
            new TopicDefinition("orders.completed.v1", 3, (short) 1, BUSINESS_CONFIG),
            new TopicDefinition("orders.failed.v1", 3, (short) 1, BUSINESS_CONFIG),
            new TopicDefinition("orders.created.v1-dlt", 1, (short) 1, DLT_CONFIG),
            new TopicDefinition("customer-order-statistics.v1", 3, (short) 1, Map.of(
                "cleanup.policy", "compact",
                "retention.ms", "-1"
            )),
            new TopicDefinition("fraud.alerts.v1", 3, (short) 1, BUSINESS_CONFIG)
        );
    }
}
