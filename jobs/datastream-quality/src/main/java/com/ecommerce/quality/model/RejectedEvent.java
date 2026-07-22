package com.ecommerce.quality.model;

import java.io.Serializable;

public class RejectedEvent implements Serializable {
    private String reasonCode;
    private String reasonMessage;
    private String rawPayload;
    private String observedAt;
    private String jobVersion;

    public RejectedEvent() {}

    public RejectedEvent(String reasonCode, String reasonMessage, String rawPayload,
            String observedAt, String jobVersion) {
        this.reasonCode = reasonCode;
        this.reasonMessage = reasonMessage;
        this.rawPayload = rawPayload;
        this.observedAt = observedAt;
        this.jobVersion = jobVersion;
    }

    public String getReasonCode() { return reasonCode; }
    public void setReasonCode(String reasonCode) { this.reasonCode = reasonCode; }
    public String getReasonMessage() { return reasonMessage; }
    public void setReasonMessage(String reasonMessage) { this.reasonMessage = reasonMessage; }
    public String getRawPayload() { return rawPayload; }
    public void setRawPayload(String rawPayload) { this.rawPayload = rawPayload; }
    public String getObservedAt() { return observedAt; }
    public void setObservedAt(String observedAt) { this.observedAt = observedAt; }
    public String getJobVersion() { return jobVersion; }
    public void setJobVersion(String jobVersion) { this.jobVersion = jobVersion; }
}
