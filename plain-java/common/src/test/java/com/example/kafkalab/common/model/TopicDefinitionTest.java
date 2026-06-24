package com.example.kafkalab.common.model;

import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class TopicDefinitionTest {

    @Test
    void shouldCreateValidTopicDefinition() {
        TopicDefinition td = new TopicDefinition("my-topic", 3, (short) 1, Map.of());
        assertThat(td.name()).isEqualTo("my-topic");
        assertThat(td.partitions()).isEqualTo(3);
        assertThat(td.replicationFactor()).isEqualTo((short) 1);
    }

    @Test
    void shouldDefaultConfigurationToEmptyMapWhenNull() {
        TopicDefinition td = new TopicDefinition("my-topic", 3, (short) 1, null);
        assertThat(td.configuration()).isEmpty();
    }

    @Test
    void shouldRejectBlankName() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> new TopicDefinition("", 3, (short) 1, null));
    }

    @Test
    void shouldRejectZeroPartitions() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> new TopicDefinition("topic", 0, (short) 1, null));
    }

    @Test
    void shouldRejectZeroReplicationFactor() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> new TopicDefinition("topic", 1, (short) 0, null));
    }
}
