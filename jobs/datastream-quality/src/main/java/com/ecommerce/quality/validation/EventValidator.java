package com.ecommerce.quality.validation;

import com.ecommerce.quality.model.UserBehaviorEvent;
import com.ecommerce.quality.model.ValidationResult;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Set;

public final class EventValidator {
    private static final int MAX_FIELD_LENGTH = 256;
    private static final Set<String> EVENT_TYPES = Set.of("view", "click", "cart", "purchase", "order", "pay");

    public ValidationResult validate(UserBehaviorEvent event, Instant observedAt) {
        String[] values = {event.getEventId(), event.getUserId(), event.getProductId(), event.getEventType(),
                event.getEventTime(), event.getChannel(), event.getDeviceType(), event.getPageId()};
        for (String value : values) {
            if (value == null || value.isBlank()) {
                return ValidationResult.invalid("MISSING_REQUIRED_FIELD", "all eight event fields are required");
            }
            if (value.length() > MAX_FIELD_LENGTH) {
                return ValidationResult.invalid("FIELD_TOO_LONG", "event field exceeds 256 characters");
            }
        }
        if (!EVENT_TYPES.contains(event.getEventType())) {
            return ValidationResult.invalid("INVALID_EVENT_TYPE", "event_type is not supported");
        }

        final Instant eventInstant;
        try {
            eventInstant = OffsetDateTime.parse(event.getEventTime()).toInstant();
        } catch (DateTimeException exception) {
            return ValidationResult.invalid("INVALID_EVENT_TIME", "event_time must be ISO-8601 with timezone");
        }
        if (eventInstant.isAfter(observedAt.plus(Duration.ofMinutes(5)))) {
            return ValidationResult.invalid("FUTURE_EVENT_TIME", "event_time is more than five minutes in the future");
        }
        return ValidationResult.valid(event);
    }
}
