package com.ecommerce.quality.model;

import java.util.Objects;

public final class ValidationResult {
    private final UserBehaviorEvent event;
    private final String reasonCode;
    private final String reasonMessage;

    private ValidationResult(UserBehaviorEvent event, String reasonCode, String reasonMessage) {
        this.event = event;
        this.reasonCode = reasonCode;
        this.reasonMessage = reasonMessage;
    }

    public static ValidationResult valid(UserBehaviorEvent event) {
        return new ValidationResult(Objects.requireNonNull(event), null, null);
    }

    public static ValidationResult invalid(String reasonCode, String reasonMessage) {
        return new ValidationResult(null, Objects.requireNonNull(reasonCode), Objects.requireNonNull(reasonMessage));
    }

    public boolean isValid() { return event != null; }
    public UserBehaviorEvent getEvent() { return event; }
    public String getReasonCode() { return reasonCode; }
    public String getReasonMessage() { return reasonMessage; }
}
