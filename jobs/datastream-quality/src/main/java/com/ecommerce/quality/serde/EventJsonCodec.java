package com.ecommerce.quality.serde;

import com.ecommerce.quality.model.UserBehaviorEvent;
import com.ecommerce.quality.model.ValidationResult;
import com.ecommerce.quality.validation.EventValidator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import java.time.Instant;

public final class EventJsonCodec {
    private final ObjectMapper mapper;
    private final EventValidator validator;

    public EventJsonCodec() {
        mapper = new ObjectMapper();
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        validator = new EventValidator();
    }

    public ValidationResult parseAndValidate(String payload, Instant observedAt) {
        try {
            JsonNode root = mapper.readTree(payload);
            if (root == null || !root.isObject()) {
                return malformed();
            }
            UserBehaviorEvent event = mapper.treeToValue(root, UserBehaviorEvent.class);
            return validator.validate(event, observedAt);
        } catch (JsonProcessingException | IllegalArgumentException exception) {
            return malformed();
        }
    }

    public String toJson(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("standard event serialization failed", exception);
        }
    }

    private static ValidationResult malformed() {
        return ValidationResult.invalid("MALFORMED_JSON", "payload must be a JSON object");
    }
}
