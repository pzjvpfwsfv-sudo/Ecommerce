package com.ecommerce.quality.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class EventModelTest {
    @Test
    void eventBeanPreservesTheEightFieldContract() {
        UserBehaviorEvent event = new UserBehaviorEvent(
                "evt-1", "user-1", "product-1", "view",
                "2026-07-22T10:00:00Z", "app", "android", "home");
        UserBehaviorEvent copy = new UserBehaviorEvent(
                "evt-1", "user-1", "product-1", "view",
                "2026-07-22T10:00:00Z", "app", "android", "home");

        assertEquals(copy, event);
        assertEquals(copy.hashCode(), event.hashCode());
        assertEquals("home", event.getPageId());
    }

    @Test
    void validationResultHasMutuallyExclusiveStates() {
        UserBehaviorEvent event = new UserBehaviorEvent();

        ValidationResult valid = ValidationResult.valid(event);
        ValidationResult invalid = ValidationResult.invalid("INVALID_EVENT_TYPE", "event_type is invalid");

        assertTrue(valid.isValid());
        assertEquals(event, valid.getEvent());
        assertNull(valid.getReasonCode());
        assertFalse(invalid.isValid());
        assertNull(invalid.getEvent());
        assertEquals("INVALID_EVENT_TYPE", invalid.getReasonCode());
        assertEquals("event_type is invalid", invalid.getReasonMessage());
    }
}
