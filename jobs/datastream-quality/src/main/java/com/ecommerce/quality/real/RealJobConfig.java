package com.ecommerce.quality.real;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.java.utils.ParameterTool;

public record RealJobConfig(String bootstrapServers, String runId, String inputTopic, String cleanTopic,
                            String dlqTopic, String lateTopic, String checkpointUri, Duration stateTtl) {
    public static RealJobConfig fromArgs(String[] args) {
        var p = ParameterTool.fromArgs(args);
        var allowed = Set.of("bootstrap-servers", "run-id", "input-topic", "clean-topic", "dlq-topic",
                "late-topic", "checkpoint-uri", "state-ttl-hours");
        if (!allowed.containsAll(p.toMap().keySet())) throw new IllegalArgumentException("unknown real-job option");
        String bootstrap = required(p, "bootstrap-servers");
        String id = required(p, "run-id");
        if (!id.matches("[a-z0-9][a-z0-9_-]{0,31}")) throw new IllegalArgumentException("invalid isolated run-id");
        String checkpoint = required(p, "checkpoint-uri");
        if (!checkpoint.equals("s3a://flink-state/checkpoints/graduation-g2b/" + id)) {
            throw new IllegalArgumentException("checkpoint must be the dedicated G2-B run-id path");
        }
        String input = topic(p.get("input-topic", "real_behavior_events_v1"), "events");
        String clean = topic(p.get("clean-topic", "real_behavior_clean_v1_" + id), "clean");
        String dlq = topic(p.get("dlq-topic", "real_behavior_dlq_v1_" + id), "dlq");
        String late = topic(p.get("late-topic", "real_behavior_late_v1_" + id), "late");
        if (Set.of(input, clean, dlq, late).size() != 4) throw new IllegalArgumentException("topic collision");
        long hours = p.getLong("state-ttl-hours", 24);
        if (hours < 1 || hours > 168) throw new IllegalArgumentException("state TTL must be 1 to 168 hours");
        return new RealJobConfig(bootstrap, id, input, clean, dlq, late, checkpoint, Duration.ofHours(hours));
    }

    private static String required(ParameterTool p, String name) {
        String value = p.get(name, "").trim();
        if (value.isEmpty()) throw new IllegalArgumentException("missing --" + name);
        return value;
    }

    private static String topic(String value, String kind) {
        if (!value.matches("real_behavior_" + kind + "_v1(?:_[a-z0-9][a-z0-9_-]{0,63})?")) {
            throw new IllegalArgumentException("topic is outside its dedicated real-data namespace");
        }
        return value;
    }

    public String consumerGroup() { return "graduation-g2b-" + runId; }
    public String transactionPrefix(String kind) { return "g2b-" + runId + "-" + kind + "-"; }
    public String uid(String name) { return "g2b-" + runId + "-" + name; }
    public List<String> topics() { return List.of(inputTopic, cleanTopic, dlqTopic, lateTopic); }
}
