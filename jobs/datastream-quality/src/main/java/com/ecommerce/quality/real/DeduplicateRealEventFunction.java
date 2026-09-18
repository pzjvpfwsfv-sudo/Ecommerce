package com.ecommerce.quality.real;

import java.time.Duration;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public final class DeduplicateRealEventFunction extends KeyedProcessFunction<String, RealEvent, RealEvent> {
    public static final OutputTag<String> DUPLICATE = new OutputTag<>("real-duplicate") {};
    private final Duration ttl;
    private transient ValueState<Boolean> seen;
    private transient RealEventCodec codec;
    private transient Counter duplicates;

    public DeduplicateRealEventFunction(Duration ttl) {
        if (ttl.isNegative() || ttl.isZero()) throw new IllegalArgumentException("TTL must be positive");
        this.ttl = ttl;
    }

    @Override
    public void open(OpenContext context) {
        var descriptor = new ValueStateDescriptor<>("real-seen-event-id-v1", Types.BOOLEAN);
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(ttl)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build());
        seen = getRuntimeContext().getState(descriptor);
        codec = new RealEventCodec();
        duplicates = getRuntimeContext().getMetricGroup().counter("duplicate_events_total");
    }

    @Override
    public void processElement(RealEvent event, Context context, Collector<RealEvent> output) throws Exception {
        if (Boolean.TRUE.equals(seen.value())) {
            duplicates.inc();
            context.output(DUPLICATE, codec.rejection(event.envelope(), "DUPLICATE_EVENT"));
            return;
        }
        seen.update(Boolean.TRUE);
        output.collect(event);
    }
}
