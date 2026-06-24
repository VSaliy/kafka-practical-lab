package com.example.kafkalab.common.config;

public final class KafkaDefaults {
    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String ORDER_TOPIC = "orders.created.v1";
    public static final int DEFAULT_PARTITIONS = 3;
    public static final short DEFAULT_REPLICATION_FACTOR = 1;
    public static final String RETENTION_MS_7_DAYS = "604800000";

    private KafkaDefaults() {
    }
}
