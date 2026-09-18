package com.ecommerce.quality.real;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.admin.AdminClient;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class RealDataQualityJob {
    private RealDataQualityJob() {}

    public static void main(String[] args) throws Exception {
        RealJobConfig config = RealJobConfig.fromArgs(args);
        checkTopics(config);
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env, config);
        env.execute(config.consumerGroup());
    }

    public static void build(StreamExecutionEnvironment env, RealJobConfig config) {
        env.setParallelism(1);
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage(config.checkpointUri());
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(15)));

        var source = KafkaSource.<KafkaEnvelope>builder()
                .setBootstrapServers(config.bootstrapServers()).setTopics(config.inputTopic())
                .setGroupId(config.consumerGroup())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setProperty("allow.auto.create.topics", "false")
                .setProperty("isolation.level", "read_committed")
                .setProperty("fetch.max.bytes", "4194304")
                .setProperty("max.partition.fetch.bytes", "1048576")
                .setDeserializer(new RealKafkaDeserializer()).build();
        var raw = env.fromSource(source, WatermarkStrategy.noWatermarks(), "real-source")
                .uid(config.uid("source"));
        var parsed = raw.process(new ParseRealEventFunction()).name("real-parse").uid(config.uid("parse"));
        var unique = parsed.keyBy(RealEvent::identityKey)
                .process(new DeduplicateRealEventFunction(config.stateTtl()))
                .name("real-deduplicate").uid(config.uid("deduplicate"));
        var timed = unique.assignTimestampsAndWatermarks(WatermarkStrategy
                .<RealEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, previous) -> event.eventTimeMillis)
                .withIdleness(Duration.ofSeconds(30)))
                .name("real-event-time").uid(config.uid("event-time"));
        var clean = timed.process(new RouteRealEventFunction()).name("real-route").uid(config.uid("route"));
        var rejected = parsed.getSideOutput(ParseRealEventFunction.REJECTED)
                .union(unique.getSideOutput(DeduplicateRealEventFunction.DUPLICATE));
        clean.sinkTo(sink(config, config.cleanTopic(), "clean"))
                .name("real-clean-sink").uid(config.uid("clean-sink"));
        rejected.sinkTo(sink(config, config.dlqTopic(), "dlq"))
                .name("real-dlq-sink").uid(config.uid("dlq-sink"));
        clean.getSideOutput(RouteRealEventFunction.LATE).sinkTo(sink(config, config.lateTopic(), "late"))
                .name("real-late-sink").uid(config.uid("late-sink"));
    }

    private static KafkaSink<String> sink(RealJobConfig config, String topic, String kind) {
        return KafkaSink.<String>builder().setBootstrapServers(config.bootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema()).build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(config.transactionPrefix(kind))
                .setProperty("transaction.timeout.ms", "900000").build();
    }

    private static void checkTopics(RealJobConfig config) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", config.bootstrapServers());
        properties.setProperty("request.timeout.ms", "10000");
        properties.setProperty("default.api.timeout.ms", "10000");
        AdminClient client = AdminClient.create(properties);
        try {
            client.describeTopics(config.topics()).allTopicNames().get(12, TimeUnit.SECONDS);
        } finally {
            client.close(Duration.ofSeconds(2));
        }
    }
}
