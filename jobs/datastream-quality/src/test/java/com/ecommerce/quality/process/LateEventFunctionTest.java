package com.ecommerce.quality.process;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ecommerce.quality.model.LateEvent;
import com.ecommerce.quality.model.UserBehaviorEvent;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.Test;

class LateEventFunctionTest {
    @Test
    void routesEventAtCurrentWatermarkOnlyToLateOutput() throws Exception {
        try (OneInputStreamOperatorTestHarness<UserBehaviorEvent, UserBehaviorEvent> harness =
                ProcessFunctionTestHarnesses.forProcessFunction(new LateEventFunction())) {
            harness.processWatermark(new Watermark(100L));
            harness.processElement(event("evt-late"), 90L);

            assertEquals(0, harness.extractOutputStreamRecords().size());
            StreamRecord<LateEvent> late = harness.getSideOutput(LateEventFunction.LATE_TAG).poll();
            assertEquals("evt-late", late.getValue().getEvent().getEventId());
            assertEquals(100L, late.getValue().getWatermark());
            assertEquals(10L, late.getValue().getLatenessMs());
        }
    }

    static UserBehaviorEvent event(String id) {
        return new UserBehaviorEvent(id, "u-1", "p-1", "view", "2026-07-22T10:00:00Z",
                "app", "android", "home");
    }
}
