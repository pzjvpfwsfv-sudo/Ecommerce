package com.ecommerce.quality.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class JobConfigTest {
    @Test
    void defaultsToShadowWithProductionReliabilityDurations() {
        JobConfig config = JobConfig.fromArgs(requiredArgs());

        assertEquals("shadow", config.mode());
        assertEquals("user_behavior_clean_shadow", config.cleanTopic());
        assertEquals(Duration.ofSeconds(10), config.watermarkOutOfOrderness());
        assertEquals(Duration.ofSeconds(30), config.sourceIdleness());
        assertEquals(Duration.ofHours(24), config.stateTtl());
        assertNotEquals(config.cleanTransactionPrefix(), config.dlqTransactionPrefix());
        assertNotEquals(config.dlqTransactionPrefix(), config.lateTransactionPrefix());
    }

    @Test
    void productionModeSelectsFormalCleanTopic() {
        JobConfig config = JobConfig.fromArgs(concat(requiredArgs(), "--mode", "production"));
        assertEquals("user_behavior_clean", config.cleanTopic());
    }

    @Test
    void rejectsInvalidModeBlankBootstrapAndConflictingTopics() {
        assertThrows(IllegalArgumentException.class,
                () -> JobConfig.fromArgs(concat(requiredArgs(), "--mode", "unsafe")));
        assertThrows(IllegalArgumentException.class,
                () -> JobConfig.fromArgs(new String[] {"--bootstrap-servers", "", "--checkpoint-uri", "file:///tmp/cp"}));
        assertThrows(IllegalArgumentException.class,
                () -> JobConfig.fromArgs(concat(requiredArgs(), "--dlq-topic", "user_behavior_clean_shadow")));
    }

    private static String[] requiredArgs() {
        return new String[] {"--bootstrap-servers", "kafka:29092", "--checkpoint-uri", "file:///tmp/checkpoints/chapter-9"};
    }

    private static String[] concat(String[] base, String... extra) {
        String[] result = new String[base.length + extra.length];
        System.arraycopy(base, 0, result, 0, base.length);
        System.arraycopy(extra, 0, result, base.length, extra.length);
        return result;
    }
}
