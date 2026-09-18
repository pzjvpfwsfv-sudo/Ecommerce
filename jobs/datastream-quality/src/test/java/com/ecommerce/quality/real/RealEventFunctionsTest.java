package com.ecommerce.quality.real;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

class RealEventFunctionsTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void parsesValidAndRoutesInvalidWithOffset() throws Exception {
        try (var h = ProcessFunctionTestHarnesses.forProcessFunction(new ParseRealEventFunction())) {
            h.processElement(RealEventCodecTest.envelope(RealEventCodecTest.BASIC), 0);
            h.processElement(RealEventCodecTest.envelope("{bad"), 0);
            assertEquals(1, h.extractOutputStreamRecords().size());
            var rejected = mapper.readTree(h.getSideOutput(ParseRealEventFunction.REJECTED).poll().getValue());
            assertEquals("MALFORMED_JSON", rejected.get("reason_code").asText());
            assertEquals(37, rejected.get("source_offset").asLong());
        }
    }

    @Test
    void ttlAndSnapshotRestorePreserveDuplicateBoundary() throws Exception {
        RealEvent event = new RealEventCodec().parse(RealEventCodecTest.envelope(RealEventCodecTest.BASIC));
        OperatorSubtaskState snapshot;
        try (var h = dedup()) {
            h.open();
            h.setStateTtlProcessingTime(0);
            h.processElement(event, event.eventTimeMillis);
            h.processElement(event, event.eventTimeMillis);
            assertEquals(1, h.extractOutputStreamRecords().size());
            assertEquals("DUPLICATE_EVENT", mapper.readTree(h.getSideOutput(DeduplicateRealEventFunction.DUPLICATE)
                    .poll().getValue()).get("reason_code").asText());
            snapshot = h.snapshot(1L, 1L);
        }
        try (var h = dedup()) {
            h.initializeState(snapshot);
            h.open();
            h.setStateTtlProcessingTime(500);
            h.processElement(event, event.eventTimeMillis);
            assertEquals(0, h.extractOutputStreamRecords().size());
            assertEquals(1, h.getSideOutput(DeduplicateRealEventFunction.DUPLICATE).size());
            h.setStateTtlProcessingTime(1001);
            h.processElement(event, event.eventTimeMillis);
            assertEquals(1, h.extractOutputStreamRecords().size());
        }
    }

    @Test
    void historicalDataIsNotLateUntilItsEventTimeWatermarkPasses() throws Exception {
        RealEvent event = new RealEventCodec().parse(RealEventCodecTest.envelope(RealEventCodecTest.BASIC));
        try (OneInputStreamOperatorTestHarness<RealEvent, String> h =
                     ProcessFunctionTestHarnesses.forProcessFunction(new RouteRealEventFunction())) {
            h.processElement(event, event.eventTimeMillis);
            assertEquals(1, h.extractOutputStreamRecords().size());
            assertEquals(mapper.readTree(event.json), mapper.readTree(h.extractOutputValues().get(0)));
            h.processWatermark(new Watermark(event.eventTimeMillis));
            h.processElement(event, event.eventTimeMillis);
            assertEquals(1, h.extractOutputStreamRecords().size());
            var late = mapper.readTree(h.getSideOutput(RouteRealEventFunction.LATE).poll().getValue());
            assertEquals(0, late.get("lateness_ms").asLong());
            assertEquals(mapper.readTree(event.json), late.get("event"));
            assertEquals(37, late.get("source_offset").asLong());
        }
    }

    private KeyedOneInputStreamOperatorTestHarness<String, RealEvent, RealEvent> dedup() throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new DeduplicateRealEventFunction(Duration.ofSeconds(1))),
                RealEvent::identityKey, Types.STRING);
    }
}
