package com.example.kafkalab.admin;

import com.example.kafkalab.common.config.KafkaDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminClientMain {

    private static final Logger log = LoggerFactory.getLogger(AdminClientMain.class);

    public static void main(String[] args) {
        String bootstrapServers = KafkaDefaults.DEFAULT_BOOTSTRAP_SERVERS;
        short replicationFactor = KafkaDefaults.DEFAULT_REPLICATION_FACTOR;

        for (int i = 0; i < args.length - 1; i++) {
            if ("--bootstrap-servers".equals(args[i])) {
                bootstrapServers = args[i + 1];
            } else if ("--replication-factor".equals(args[i])) {
                replicationFactor = Short.parseShort(args[i + 1]);
            }
        }

        log.info("Provisioning topics on {} with replication factor {}", bootstrapServers, replicationFactor);

        try {
            new TopicProvisioner().provision(bootstrapServers, TopicCatalog.all(replicationFactor));
            log.info("Topic provisioning completed successfully");
        } catch (Exception e) {
            log.error("Topic provisioning failed", e);
            System.exit(1);
        }
    }
}
