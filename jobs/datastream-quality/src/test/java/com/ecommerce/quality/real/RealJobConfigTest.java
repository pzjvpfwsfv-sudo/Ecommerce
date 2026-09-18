package com.ecommerce.quality.real;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class RealJobConfigTest {
    static String[] args(String... extra) {
        var values = new ArrayList<>(List.of("--bootstrap-servers", "kafka:29092", "--run-id", "verify-01",
                "--checkpoint-uri", "s3a://flink-state/checkpoints/graduation-g2b/verify-01"));
        values.addAll(List.of(extra));
        return values.toArray(String[]::new);
    }

    @Test
    void derivesIsolatedResourceNames() {
        var c = RealJobConfig.fromArgs(args());
        assertEquals("real_behavior_events_v1", c.inputTopic());
        assertEquals("real_behavior_clean_v1_verify-01", c.cleanTopic());
        assertEquals("graduation-g2b-verify-01", c.consumerGroup());
        assertEquals(Duration.ofHours(24), c.stateTtl());
        assertNotEquals(c.transactionPrefix("clean"), c.transactionPrefix("dlq"));
        assertEquals(4, c.topics().size());
    }

    @Test
    void rejectsOldNamespacesUnsafeStateAndTypos() {
        for (String[] bad : new String[][] {
                {"--input-topic", "user_behavior_events"}, {"--clean-topic", "user_behavior_clean"},
                {"--dlq-topic", "real_behavior_clean_v1_x"}, {"--late-topic", "real_behavior_events_v1"},
                {"--input-topic", "real_behavior_events_v10"},
                {"--run-id", "../chapter9"}, {"--run-id", "CHAPTER9"},
                {"--checkpoint-uri", "file:///tmp/checkpoints"},
                {"--checkpoint-uri", "s3a://flink-state/checkpoints/chapter-9"},
                {"--checkpoint-uri", "s3a://flink-state/checkpoints/graduation-g2b/verify-01/../old"},
                {"--checkpoint-uri", "s3a://flink-state/checkpoints/graduation-g2b/different"},
                {"--state-ttl-hours", "0"}, {"--state-ttl-hours", "169"},
                {"--boostrap-servers", "typo"}, {"--bootstrap-servers", ""}}) {
            assertThrows(IllegalArgumentException.class, () -> RealJobConfig.fromArgs(args(bad)), List.of(bad).toString());
        }
        assertThrows(IllegalArgumentException.class, () -> RealJobConfig.fromArgs(new String[0]));
    }
}
