package com.ecommerce.quality;

import com.ecommerce.quality.config.JobConfig;
import com.ecommerce.quality.model.LateEvent;
import com.ecommerce.quality.model.RejectedEvent;
import com.ecommerce.quality.model.UserBehaviorEvent;
import com.ecommerce.quality.process.DeduplicateFunction;
import com.ecommerce.quality.process.LateEventFunction;
import com.ecommerce.quality.process.ParseAndValidateFunction;
import com.ecommerce.quality.serde.JsonKafkaRecordSerializationSchema;
import java.time.OffsetDateTime;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public final class DataQualityJob {
    private DataQualityJob() {}

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env, config);
        env.execute("chapter-9-datastream-quality-" + config.mode());
    }

    public static void build(StreamExecutionEnvironment env, JobConfig config) {
        configureReliability(env, config);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(config.inputTopic())
                .setGroupId(config.consumerGroup())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> raw = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-raw-source")
                .uid("chapter9-raw-source");
        SingleOutputStreamOperator<UserBehaviorEvent> parsed = raw
                .process(new ParseAndValidateFunction(config.jobVersion()))
                .name("parse-and-validate").uid("chapter9-parse-validate");
        DataStream<RejectedEvent> parseRejected = parsed.getSideOutput(ParseAndValidateFunction.REJECTED_TAG);

        WatermarkStrategy<UserBehaviorEvent> watermarks = WatermarkStrategy
                .<UserBehaviorEvent>forBoundedOutOfOrderness(config.watermarkOutOfOrderness())
                .withTimestampAssigner((event, previous) -> OffsetDateTime.parse(event.getEventTime()).toInstant().toEpochMilli())
                .withIdleness(config.sourceIdleness());
        SingleOutputStreamOperator<UserBehaviorEvent> routed = parsed
                .assignTimestampsAndWatermarks(watermarks)
                .process(new LateEventFunction())
                .name("route-late-events").uid("chapter9-route-late");
        DataStream<LateEvent> late = routed.getSideOutput(LateEventFunction.LATE_TAG);

        SingleOutputStreamOperator<UserBehaviorEvent> clean = routed
                .keyBy(UserBehaviorEvent::getEventId)
                .process(new DeduplicateFunction(config.stateTtl(), config.jobVersion()))
                .name("deduplicate-event-id").uid("chapter9-deduplicate");
        DataStream<RejectedEvent> duplicateRejected = clean.getSideOutput(DeduplicateFunction.DUPLICATE_TAG);
        DataStream<RejectedEvent> dlq = parseRejected.union(duplicateRejected);

        clean.sinkTo(jsonSink(config, config.cleanTopic(), config.cleanTransactionPrefix()))
                .name("kafka-clean-sink").uid("chapter9-clean-sink");
        dlq.sinkTo(jsonSink(config, config.dlqTopic(), config.dlqTransactionPrefix()))
                .name("kafka-dlq-sink").uid("chapter9-dlq-sink");
        late.sinkTo(jsonSink(config, config.lateTopic(), config.lateTransactionPrefix()))
                .name("kafka-late-sink").uid("chapter9-late-sink");
    }

    private static <T> KafkaSink<T> jsonSink(JobConfig config, String topic, String transactionPrefix) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setRecordSerializer(new JsonKafkaRecordSerializationSchema<>(topic))
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(transactionPrefix)
                .setProperty("transaction.timeout.ms", "900000")
                .build();
    }

    private static void configureReliability(StreamExecutionEnvironment env, JobConfig config) {
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(10_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60_000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage(config.checkpointUri());
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));
    }
}
