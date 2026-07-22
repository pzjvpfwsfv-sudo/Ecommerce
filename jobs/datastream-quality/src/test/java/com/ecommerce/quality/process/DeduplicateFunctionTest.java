package com.ecommerce.quality.process;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ecommerce.quality.model.RejectedEvent;
import com.ecommerce.quality.model.UserBehaviorEvent;
import java.time.Duration;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.Test;

class DeduplicateFunctionTest {
    @Test
    void rejectsDuplicateWithinTtlAndAllowsItAfterExpiry() throws Exception {
        KeyedProcessOperator<String, UserBehaviorEvent, UserBehaviorEvent> operator =
                new KeyedProcessOperator<>(new DeduplicateFunction(Duration.ofHours(24), "chapter-9-v1"));
        try (KeyedOneInputStreamOperatorTestHarness<String, UserBehaviorEvent, UserBehaviorEvent> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(operator,
                        UserBehaviorEvent::getEventId, Types.STRING)) {
            harness.open();
            UserBehaviorEvent event = LateEventFunctionTest.event("evt-duplicate");
            harness.setProcessingTime(0L);
            harness.setStateTtlProcessingTime(0L);
            harness.processElement(event, 1L);
            harness.processElement(event, 2L);

            assertEquals(1, harness.extractOutputStreamRecords().size());
            StreamRecord<RejectedEvent> duplicate = harness.getSideOutput(DeduplicateFunction.DUPLICATE_TAG).poll();
            assertEquals("DUPLICATE_EVENT", duplicate.getValue().getReasonCode());

            harness.setStateTtlProcessingTime(Duration.ofHours(24).toMillis() + 1L);
            harness.processElement(event, 3L);
            assertEquals(2, harness.extractOutputStreamRecords().size());
        }
    }
}
