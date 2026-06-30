package com.example.kafkalab.transactional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionalProcessorMain {

    private static final Logger log = LoggerFactory.getLogger(TransactionalProcessorMain.class);

    public static void main(String[] args) {
        TransactionalProcessorSettings defaults = TransactionalProcessorSettings.defaults();
        String bootstrapServers = defaults.bootstrapServers();
        String inputTopic = defaults.inputTopic();
        String completedTopic = defaults.completedTopic();
        String failedTopic = defaults.failedTopic();
        String groupId = defaults.groupId();
        String transactionalId = defaults.transactionalId();

        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--bootstrap-servers" -> bootstrapServers = args[i + 1];
                case "--input-topic" -> inputTopic = args[i + 1];
                case "--completed-topic" -> completedTopic = args[i + 1];
                case "--failed-topic" -> failedTopic = args[i + 1];
                case "--group-id" -> groupId = args[i + 1];
                case "--transactional-id" -> transactionalId = args[i + 1];
                default -> {
                    // ignore unknown arguments for simple lab CLI compatibility
                }
            }
        }

        TransactionalProcessorSettings settings = new TransactionalProcessorSettings(
            bootstrapServers,
            inputTopic,
            completedTopic,
            failedTopic,
            groupId,
            transactionalId
        );
        log.info("Starting transactional processor: input={}, completed={}, failed={}, group={}, transactionalId={}",
            inputTopic, completedTopic, failedTopic, groupId, transactionalId);

        try (TransactionalOrderProcessor processor = new TransactionalOrderProcessor(settings)) {
            Runtime.getRuntime().addShutdownHook(new Thread(processor::shutdown));
            processor.run();
        } catch (Exception e) {
            log.error("Transactional processor failed", e);
            System.exit(1);
        }
    }
}
