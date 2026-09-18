package com.ecommerce.quality.real;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public final class RouteRealEventFunction extends ProcessFunction<RealEvent, String> {
    public static final OutputTag<String> LATE = new OutputTag<>("real-late") {};
    private transient RealEventCodec codec;
    private transient Counter clean;
    private transient Counter late;

    @Override
    public void open(OpenContext context) {
        codec = new RealEventCodec();
        clean = getRuntimeContext().getMetricGroup().counter("clean_events_total");
        late = getRuntimeContext().getMetricGroup().counter("late_events_total");
    }

    @Override
    public void processElement(RealEvent event, Context context, Collector<String> output) {
        long watermark = context.timerService().currentWatermark();
        if (event.eventTimeMillis <= watermark) {
            late.inc();
            context.output(LATE, codec.late(event, watermark));
        } else {
            clean.inc();
            output.collect(event.json);
        }
    }
}
