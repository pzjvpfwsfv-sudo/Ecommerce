package com.ecommerce.quality.real;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public final class ParseRealEventFunction extends ProcessFunction<KafkaEnvelope, RealEvent> {
    public static final OutputTag<String> REJECTED = new OutputTag<>("real-invalid") {};
    private transient RealEventCodec codec;
    private transient Counter parsed;
    private transient Counter invalid;

    @Override
    public void open(OpenContext context) {
        codec = new RealEventCodec();
        parsed = getRuntimeContext().getMetricGroup().counter("parsed_events_total");
        invalid = getRuntimeContext().getMetricGroup().counter("invalid_events_total");
    }

    @Override
    public void processElement(KafkaEnvelope record, Context context, Collector<RealEvent> output) {
        final RealEvent event;
        try {
            event = codec.parse(record);
        } catch (RealEventCodec.InvalidEvent exception) {
            invalid.inc();
            context.output(REJECTED, codec.rejection(record, exception.code()));
            return;
        }
        parsed.inc();
        output.collect(event);
    }
}
