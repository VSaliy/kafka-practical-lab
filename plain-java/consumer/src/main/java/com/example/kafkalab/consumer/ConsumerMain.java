package com.example.kafkalab.consumer;

import com.example.kafkalab.common.config.KafkaDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerMain {

    private static final Logger log = LoggerFactory.getLogger(ConsumerMain.class);

    public static void main(String[] args) {
        String bootstrapServers = KafkaDefaults.DEFAULT_BOOTSTRAP_SERVERS;
        String topic = KafkaDefaults.ORDER_TOPIC;
        String groupId = "order-audit-consumer";
        long processingDelayMs = 0;
        double failureRate = 0.0;

        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--bootstrap-servers" -> bootstrapServers = args[i + 1];
                case "--topic" -> topic = args[i + 1];
                case "--group-id" -> groupId = args[i + 1];
                case "--processing-delay-ms" -> processingDelayMs = Long.parseLong(args[i + 1]);
                case "--failure-rate" -> failureRate = Double.parseDouble(args[i + 1]);
                default -> {
                    // ignore
                }
            }
        }

        ConsumerSettings settings = new ConsumerSettings(bootstrapServers, topic, groupId, processingDelayMs, failureRate);
        log.info("Starting consumer: bootstrap={}, topic={}, group={}", bootstrapServers, topic, groupId);

        try (OrderEventConsumer consumer = new OrderEventConsumer(settings)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown signal received");
                consumer.shutdown();
            }));
            consumer.run();
        } catch (Exception e) {
            log.error("Consumer failed", e);
            System.exit(1);
        }
    }
}
