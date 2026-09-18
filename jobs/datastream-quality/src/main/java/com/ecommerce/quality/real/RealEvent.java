package com.ecommerce.quality.real;

import java.nio.charset.StandardCharsets;

public class RealEvent {
    public String json;
    public String datasetId;
    public String eventId;
    public String userId;
    public long eventTimeMillis;
    public String topic;
    public int partition;
    public long offset;

    public RealEvent() {}

    public String identityKey() { return datasetId + ":" + eventId; }

    public KafkaEnvelope envelope() {
        return new KafkaEnvelope(topic, partition, offset, json.getBytes(StandardCharsets.UTF_8));
    }
}
