package com.ecommerce.quality.serde;

import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class JsonKafkaRecordSerializationSchema<T> implements KafkaRecordSerializationSchema<T> {
    private final String topic;
    private transient EventJsonCodec codec;

    public JsonKafkaRecordSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(T element, KafkaSinkContext context, Long timestamp) {
        if (codec == null) {
            codec = new EventJsonCodec();
        }
        return new ProducerRecord<>(topic, null, timestamp, null,
                codec.toJson(element).getBytes(StandardCharsets.UTF_8));
    }
}
