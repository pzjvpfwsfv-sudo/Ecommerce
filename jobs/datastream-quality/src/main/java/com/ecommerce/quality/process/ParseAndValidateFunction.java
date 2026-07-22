package com.ecommerce.quality.process;

import com.ecommerce.quality.model.RejectedEvent;
import com.ecommerce.quality.model.UserBehaviorEvent;
import com.ecommerce.quality.model.ValidationResult;
import com.ecommerce.quality.serde.EventJsonCodec;
import java.time.Instant;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public final class ParseAndValidateFunction extends ProcessFunction<String, UserBehaviorEvent> {
    public static final OutputTag<RejectedEvent> REJECTED_TAG = new OutputTag<>("rejected-events") {};

    private final String jobVersion;
    private transient EventJsonCodec codec;
    private transient Counter parseErrors;
    private transient Counter validationErrors;

    public ParseAndValidateFunction(String jobVersion) {
        this.jobVersion = jobVersion;
    }

    @Override
    public void open(OpenContext openContext) {
        codec = new EventJsonCodec();
        parseErrors = getRuntimeContext().getMetricGroup().counter("parse_errors_total");
        validationErrors = getRuntimeContext().getMetricGroup().counter("validation_errors_total");
    }

    @Override
    public void processElement(String payload, Context context, Collector<UserBehaviorEvent> output) {
        Instant observedAt = Instant.now();
        ValidationResult result = codec.parseAndValidate(payload, observedAt);
        if (result.isValid()) {
            output.collect(result.getEvent());
            return;
        }
        if ("MALFORMED_JSON".equals(result.getReasonCode())) {
            parseErrors.inc();
        } else {
            validationErrors.inc();
        }
        context.output(REJECTED_TAG, new RejectedEvent(result.getReasonCode(), result.getReasonMessage(),
                payload, observedAt.toString(), jobVersion));
    }
}
