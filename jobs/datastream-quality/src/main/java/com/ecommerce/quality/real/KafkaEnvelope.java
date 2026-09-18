package com.ecommerce.quality.real;

public class KafkaEnvelope {
    public String topic;
    public int partition;
    public long offset;
    public byte[] payload;

    public KafkaEnvelope() {}

    public KafkaEnvelope(String topic, int partition, long offset, byte[] payload) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.payload = payload;
    }
}
