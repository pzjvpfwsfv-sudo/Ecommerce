package com.ecommerce.quality.model;

import java.io.Serializable;

public class LateEvent implements Serializable {
    private UserBehaviorEvent event;
    private long watermark;
    private long latenessMs;
    private String observedAt;

    public LateEvent() {}

    public LateEvent(UserBehaviorEvent event, long watermark, long latenessMs, String observedAt) {
        this.event = event;
        this.watermark = watermark;
        this.latenessMs = latenessMs;
        this.observedAt = observedAt;
    }

    public UserBehaviorEvent getEvent() { return event; }
    public void setEvent(UserBehaviorEvent event) { this.event = event; }
    public long getWatermark() { return watermark; }
    public void setWatermark(long watermark) { this.watermark = watermark; }
    public long getLatenessMs() { return latenessMs; }
    public void setLatenessMs(long latenessMs) { this.latenessMs = latenessMs; }
    public String getObservedAt() { return observedAt; }
    public void setObservedAt(String observedAt) { this.observedAt = observedAt; }
}
