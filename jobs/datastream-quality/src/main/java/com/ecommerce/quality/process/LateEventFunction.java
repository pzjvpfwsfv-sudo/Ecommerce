package com.ecommerce.quality.process;

import com.ecommerce.quality.model.LateEvent;
import com.ecommerce.quality.model.UserBehaviorEvent;
import java.time.Instant;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public final class LateEventFunction extends ProcessFunction<UserBehaviorEvent, UserBehaviorEvent> {
    public static final OutputTag<LateEvent> LATE_TAG = new OutputTag<>("late-events") {};
    private transient Counter lateEvents;

    @Override
    public void open(OpenContext openContext) {
        lateEvents = getRuntimeContext().getMetricGroup().counter("late_events_total");
    }

    @Override
    public void processElement(UserBehaviorEvent event, Context context,
            Collector<UserBehaviorEvent> output) {
        Long timestamp = context.timestamp();
        long watermark = context.timerService().currentWatermark();
        if (timestamp != null && timestamp <= watermark) {
            lateEvents.inc();
            context.output(LATE_TAG, new LateEvent(event, watermark, watermark - timestamp, Instant.now().toString()));
            return;
        }
        output.collect(event);
    }
}
