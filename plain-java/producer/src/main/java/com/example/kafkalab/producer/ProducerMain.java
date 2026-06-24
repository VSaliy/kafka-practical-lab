package com.example.kafkalab.producer;

import com.example.kafkalab.common.config.KafkaDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerMain {

    private static final Logger log = LoggerFactory.getLogger(ProducerMain.class);

    public static void main(String[] args) {
        String bootstrapServers = KafkaDefaults.DEFAULT_BOOTSTRAP_SERVERS;
        String topic = KafkaDefaults.ORDER_TOPIC;
        int count = 10;
        int customerCount = 5;

        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--bootstrap-servers" -> bootstrapServers = args[i + 1];
                case "--topic" -> topic = args[i + 1];
                case "--count" -> count = Integer.parseInt(args[i + 1]);
                case "--customer-count" -> customerCount = Integer.parseInt(args[i + 1]);
                default -> {
                    // ignore
                }
            }
        }

        log.info("Starting producer: bootstrap={}, topic={}, count={}, customers={}",
            bootstrapServers, topic, count, customerCount);

        try (OrderEventProducer producer = new OrderEventProducer(bootstrapServers, topic)) {
            int failures = producer.sendOrders(count, customerCount);
            if (failures > 0) {
                log.error("{} messages failed to send", failures);
                System.exit(1);
            }
        } catch (Exception e) {
            log.error("Producer failed", e);
            System.exit(1);
        }
    }
}
