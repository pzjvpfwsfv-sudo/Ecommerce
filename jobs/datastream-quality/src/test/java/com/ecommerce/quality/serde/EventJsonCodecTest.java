package com.ecommerce.quality.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ecommerce.quality.model.ValidationResult;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class EventJsonCodecTest {
    private static final Instant NOW = Instant.parse("2026-07-22T10:00:00Z");
    private final EventJsonCodec codec = new EventJsonCodec();

    @Test
    void parsesSnakeCaseAndDropsUnknownFieldsFromStandardOutput() {
        String payload = "{\"event_id\":\"evt-1\",\"user_id\":\"u-1\",\"product_id\":\"p-1\"," +
                "\"event_type\":\"view\",\"event_time\":\"2026-07-22T10:00:00Z\"," +
                "\"channel\":\"app\",\"device_type\":\"android\",\"page_id\":\"home\",\"extra\":1}";

        ValidationResult result = codec.parseAndValidate(payload, NOW);

        assertTrue(result.isValid());
        String normalized = codec.toJson(result.getEvent());
        assertTrue(normalized.contains("\"event_id\":\"evt-1\""));
        assertFalse(normalized.contains("extra"));
    }

    @Test
    void rejectsMalformedAndNonObjectJsonWithStableReason() {
        assertEquals("MALFORMED_JSON", codec.parseAndValidate("{bad", NOW).getReasonCode());
        assertEquals("MALFORMED_JSON", codec.parseAndValidate("[]", NOW).getReasonCode());
    }
}
