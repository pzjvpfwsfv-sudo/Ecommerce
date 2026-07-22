package com.ecommerce.quality.process;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ecommerce.quality.model.RejectedEvent;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.Test;

class ParseAndValidateFunctionTest {
    @Test
    void routesMalformedPayloadOnlyToRejectedSideOutput() throws Exception {
        try (OneInputStreamOperatorTestHarness<String, ?> harness =
                ProcessFunctionTestHarnesses.forProcessFunction(new ParseAndValidateFunction("chapter-9-v1"))) {
            harness.processElement("{bad", 1L);

            assertEquals(0, harness.extractOutputStreamRecords().size());
            StreamRecord<RejectedEvent> rejected = harness.getSideOutput(ParseAndValidateFunction.REJECTED_TAG).poll();
            assertEquals("MALFORMED_JSON", rejected.getValue().getReasonCode());
            assertEquals("{bad", rejected.getValue().getRawPayload());
        }
    }
}
