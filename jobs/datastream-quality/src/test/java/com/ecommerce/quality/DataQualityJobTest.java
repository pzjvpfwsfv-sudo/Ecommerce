package com.ecommerce.quality;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ecommerce.quality.config.JobConfig;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class DataQualityJobTest {
    @Test
    void buildsShadowTopologyAndReliabilitySettingsWithoutExecution() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        JobConfig config = JobConfig.fromArgs(new String[] {
                "--bootstrap-servers", "kafka:29092",
                "--checkpoint-uri", "file:///tmp/checkpoints/chapter-9"
        });

        DataQualityJob.build(env, config);

        assertEquals(10_000L, env.getCheckpointConfig().getCheckpointInterval());
        assertEquals(60_000L, env.getCheckpointConfig().getCheckpointTimeout());
        assertEquals(5_000L, env.getCheckpointConfig().getMinPauseBetweenCheckpoints());
        assertEquals(1, env.getCheckpointConfig().getMaxConcurrentCheckpoints());
        assertTrue(env.getConfig().getRestartStrategy().toString().contains("PT15S"),
                () -> "restart delay must tolerate TaskManager startup: " + env.getConfig().getRestartStrategy());
        Set<String> names = env.getStreamGraph().getStreamNodes().stream()
                .map(node -> node.getOperatorName()).collect(Collectors.toSet());
        assertContains(names, "kafka-raw-source");
        assertContains(names, "parse-and-validate");
        assertContains(names, "route-late-events");
        assertContains(names, "deduplicate-event-id");
        assertContains(names, "kafka-clean-sink");
        assertContains(names, "kafka-dlq-sink");
        assertContains(names, "kafka-late-sink");
    }

    private static void assertContains(Set<String> names, String expected) {
        assertTrue(names.stream().anyMatch(name -> name.contains(expected)), () -> "missing operator " + expected);
    }
}
