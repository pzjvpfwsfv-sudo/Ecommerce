package com.ecommerce.quality.model;

import java.io.Serializable;
import java.util.Objects;

public class UserBehaviorEvent implements Serializable {
    private String eventId;
    private String userId;
    private String productId;
    private String eventType;
    private String eventTime;
    private String channel;
    private String deviceType;
    private String pageId;

    public UserBehaviorEvent() {}

    public UserBehaviorEvent(String eventId, String userId, String productId, String eventType,
            String eventTime, String channel, String deviceType, String pageId) {
        this.eventId = eventId;
        this.userId = userId;
        this.productId = productId;
        this.eventType = eventType;
        this.eventTime = eventTime;
        this.channel = channel;
        this.deviceType = deviceType;
        this.pageId = pageId;
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }
    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    public String getEventTime() { return eventTime; }
    public void setEventTime(String eventTime) { this.eventTime = eventTime; }
    public String getChannel() { return channel; }
    public void setChannel(String channel) { this.channel = channel; }
    public String getDeviceType() { return deviceType; }
    public void setDeviceType(String deviceType) { this.deviceType = deviceType; }
    public String getPageId() { return pageId; }
    public void setPageId(String pageId) { this.pageId = pageId; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof UserBehaviorEvent that)) return false;
        return Objects.equals(eventId, that.eventId) && Objects.equals(userId, that.userId)
                && Objects.equals(productId, that.productId) && Objects.equals(eventType, that.eventType)
                && Objects.equals(eventTime, that.eventTime) && Objects.equals(channel, that.channel)
                && Objects.equals(deviceType, that.deviceType) && Objects.equals(pageId, that.pageId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, userId, productId, eventType, eventTime, channel, deviceType, pageId);
    }
}
