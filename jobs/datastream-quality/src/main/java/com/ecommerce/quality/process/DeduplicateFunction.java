package com.ecommerce.quality.process;

import com.ecommerce.quality.model.RejectedEvent;
import com.ecommerce.quality.model.UserBehaviorEvent;
import com.ecommerce.quality.serde.EventJsonCodec;
import java.time.Duration;
import java.time.Instant;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public final class DeduplicateFunction extends KeyedProcessFunction<String, UserBehaviorEvent, UserBehaviorEvent> {
    public static final OutputTag<RejectedEvent> DUPLICATE_TAG = new OutputTag<>("duplicate-events") {};

    private final Duration ttl;
    private final String jobVersion;
    private transient ValueState<Boolean> seen;
    private transient EventJsonCodec codec;
    private transient Counter validEvents;
    private transient Counter duplicateEvents;
    private transient Counter dlqEvents;

    public DeduplicateFunction(Duration ttl, String jobVersion) {
        this.ttl = ttl;
        this.jobVersion = jobVersion;
    }

    @Override
    public void open(OpenContext openContext) {
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(ttl)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen-event-id", Types.BOOLEAN);
        descriptor.enableTimeToLive(ttlConfig);
        seen = getRuntimeContext().getState(descriptor);
        codec = new EventJsonCodec();
        validEvents = getRuntimeContext().getMetricGroup().counter("valid_events_total");
        duplicateEvents = getRuntimeContext().getMetricGroup().counter("duplicate_events_total");
        dlqEvents = getRuntimeContext().getMetricGroup().counter("dlq_events_total");
    }

    @Override
    public void processElement(UserBehaviorEvent event, Context context,
            Collector<UserBehaviorEvent> output) throws Exception {
        if (Boolean.TRUE.equals(seen.value())) {
            duplicateEvents.inc();
            dlqEvents.inc();
            context.output(DUPLICATE_TAG, new RejectedEvent("DUPLICATE_EVENT",
                    "event_id already exists within state TTL", codec.toJson(event),
                    Instant.now().toString(), jobVersion));
            return;
        }
        seen.update(Boolean.TRUE);
        validEvents.inc();
        output.collect(event);
    }
}
