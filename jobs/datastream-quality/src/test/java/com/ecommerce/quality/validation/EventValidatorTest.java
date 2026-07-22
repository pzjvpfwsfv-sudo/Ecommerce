package com.ecommerce.quality.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ecommerce.quality.model.UserBehaviorEvent;
import com.ecommerce.quality.model.ValidationResult;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class EventValidatorTest {
    private static final Instant NOW = Instant.parse("2026-07-22T10:00:00Z");
    private final EventValidator validator = new EventValidator();

    @Test
    void acceptsValidEventAndFiveMinuteFutureBoundary() {
        assertTrue(validator.validate(event("2026-07-22T10:05:00Z"), NOW).isValid());
    }

    @Test
    void rejectsMissingInvalidTypeInvalidTimeAndTooFarFuture() {
        UserBehaviorEvent missing = event("2026-07-22T10:00:00Z");
        missing.setUserId(" ");
        assertReason("MISSING_REQUIRED_FIELD", validator.validate(missing, NOW));

        UserBehaviorEvent type = event("2026-07-22T10:00:00Z");
        type.setEventType("refund");
        assertReason("INVALID_EVENT_TYPE", validator.validate(type, NOW));

        assertReason("INVALID_EVENT_TIME", validator.validate(event("2026-07-22 10:00:00"), NOW));
        assertReason("FUTURE_EVENT_TIME", validator.validate(event("2026-07-22T10:05:00.001Z"), NOW));
    }

    @Test
    void rejectsFieldsLongerThanContractLimit() {
        UserBehaviorEvent event = event("2026-07-22T10:00:00Z");
        event.setPageId("x".repeat(257));
        assertReason("FIELD_TOO_LONG", validator.validate(event, NOW));
    }

    private static UserBehaviorEvent event(String eventTime) {
        return new UserBehaviorEvent("evt-1", "user-1", "product-1", "view", eventTime,
                "app", "android", "home");
    }

    private static void assertReason(String expected, ValidationResult result) {
        assertEquals(expected, result.getReasonCode());
    }
}
