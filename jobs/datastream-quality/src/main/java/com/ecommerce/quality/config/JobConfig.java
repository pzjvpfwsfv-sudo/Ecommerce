package com.ecommerce.quality.config;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.api.java.utils.ParameterTool;

public record JobConfig(
        String bootstrapServers,
        String inputTopic,
        String cleanTopic,
        String dlqTopic,
        String lateTopic,
        String consumerGroup,
        String mode,
        String checkpointUri,
        Duration watermarkOutOfOrderness,
        Duration sourceIdleness,
        Duration stateTtl,
        String cleanTransactionPrefix,
        String dlqTransactionPrefix,
        String lateTransactionPrefix,
        String jobVersion) {

    public static JobConfig fromArgs(String[] args) {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        String bootstrap = parameters.get("bootstrap-servers", "").trim();
        String checkpoint = parameters.get("checkpoint-uri", "").trim();
        String mode = parameters.get("mode", "shadow").trim();
        if (bootstrap.isEmpty()) {
            throw new IllegalArgumentException("--bootstrap-servers must not be blank");
        }
        if (checkpoint.isEmpty()) {
            throw new IllegalArgumentException("--checkpoint-uri must not be blank");
        }
        if (!Set.of("shadow", "production").contains(mode)) {
            throw new IllegalArgumentException("--mode must be shadow or production");
        }

        String cleanDefault = mode.equals("shadow") ? "user_behavior_clean_shadow" : "user_behavior_clean";
        String clean = parameters.get("clean-topic", cleanDefault).trim();
        String dlq = parameters.get("dlq-topic", "user_behavior_dlq").trim();
        String late = parameters.get("late-topic", "user_behavior_late").trim();
        Set<String> outputs = new HashSet<>(Set.of(clean, dlq, late));
        if (outputs.size() != 3 || outputs.contains("")) {
            throw new IllegalArgumentException("clean, dlq and late topics must be nonblank and different");
        }

        String input = nonBlank(parameters.get("input-topic", "user_behavior_events"), "input-topic");
        String group = nonBlank(parameters.get("consumer-group", "chapter9-quality-" + mode), "consumer-group");
        String namespace = nonBlank(parameters.get("transaction-prefix", "chapter9-" + mode), "transaction-prefix");
        String version = nonBlank(parameters.get("job-version", "chapter-9-v1"), "job-version");
        Duration watermark = positiveSeconds(parameters, "watermark-seconds", 10);
        Duration idleness = positiveSeconds(parameters, "idleness-seconds", 30);
        Duration ttl = Duration.ofHours(positiveLong(parameters, "state-ttl-hours", 24));
        return new JobConfig(
                bootstrap,
                input,
                clean,
                dlq,
                late,
                group,
                mode,
                checkpoint,
                watermark,
                idleness,
                ttl,
                namespace + "-clean-",
                namespace + "-dlq-",
                namespace + "-late-",
                version);
    }

    private static String nonBlank(String value, String name) {
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("--" + name + " must not be blank");
        }
        return trimmed;
    }

    private static Duration positiveSeconds(ParameterTool parameters, String name, long defaultValue) {
        return Duration.ofSeconds(positiveLong(parameters, name, defaultValue));
    }

    private static long positiveLong(ParameterTool parameters, String name, long defaultValue) {
        long value = parameters.getLong(name, defaultValue);
        if (value <= 0) {
            throw new IllegalArgumentException("--" + name + " must be positive");
        }
        return value;
    }
}
