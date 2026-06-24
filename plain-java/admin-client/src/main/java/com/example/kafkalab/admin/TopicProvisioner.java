package com.example.kafkalab.admin;

import com.example.kafkalab.common.model.TopicDefinition;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicProvisioner {

    private static final Logger log = LoggerFactory.getLogger(TopicProvisioner.class);

    public void provision(String bootstrapServers, List<TopicDefinition> topics) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "60000");

        try (AdminClient adminClient = AdminClient.create(props)) {
            provision(adminClient, topics);
        }
    }

    void provision(AdminClient adminClient, List<TopicDefinition> topics) {
        log.info("Starting topic provisioning for {} topics", topics.size());

        Set<String> existingTopics = getExistingTopics(adminClient);
        log.info("Found {} existing topics", existingTopics.size());

        List<TopicDefinition> toCreate = topics.stream()
            .filter(topic -> !existingTopics.contains(topic.name()))
            .toList();

        List<TopicDefinition> toCheck = topics.stream()
            .filter(topic -> existingTopics.contains(topic.name()))
            .toList();

        createTopics(adminClient, toCreate);
        checkExistingTopics(adminClient, toCheck);

        log.info("Topic provisioning complete. Created: {}, Already existed: {}",
            toCreate.size(), toCheck.size());
    }

    private Set<String> getExistingTopics(AdminClient adminClient) {
        try {
            return adminClient.listTopics().names().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while listing topics", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to list topics", e.getCause());
        }
    }

    private void createTopics(AdminClient adminClient, List<TopicDefinition> topics) {
        if (topics.isEmpty()) {
            return;
        }

        List<NewTopic> newTopics = topics.stream()
            .map(topic -> new NewTopic(topic.name(), topic.partitions(), topic.replicationFactor())
                .configs(topic.configuration()))
            .toList();

        CreateTopicsResult result = adminClient.createTopics(newTopics);

        for (TopicDefinition topic : topics) {
            try {
                result.values().get(topic.name()).get();
                log.info("Created topic: {} (partitions={}, replicationFactor={})",
                    topic.name(), topic.partitions(), topic.replicationFactor());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while creating topic: " + topic.name(), e);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    log.info("Topic already exists (race condition): {}", topic.name());
                } else {
                    log.error("Failed to create topic: {}", topic.name(), e.getCause());
                    throw new RuntimeException("Failed to create topic: " + topic.name(), e.getCause());
                }
            }
        }
    }

    private void checkExistingTopics(AdminClient adminClient, List<TopicDefinition> topics) {
        if (topics.isEmpty()) {
            return;
        }

        List<String> topicNames = topics.stream().map(TopicDefinition::name).toList();

        try {
            DescribeTopicsResult describeResult = adminClient.describeTopics(topicNames);
            Map<String, TopicDescription> descriptions = describeResult.allTopicNames().get();

            for (TopicDefinition expected : topics) {
                TopicDescription actual = descriptions.get(expected.name());
                if (actual == null) {
                    log.warn("Topic {} was expected to exist but not found in description", expected.name());
                    continue;
                }

                int actualPartitions = actual.partitions().size();
                if (actualPartitions != expected.partitions()) {
                    log.warn("Topic {} has {} partitions but expected {}. Partition count cannot be decreased.",
                        expected.name(), actualPartitions, expected.partitions());
                } else {
                    log.info("Topic {} is compatible (partitions={})", expected.name(), actualPartitions);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while describing topics", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to describe topics", e.getCause());
        }
    }
}
