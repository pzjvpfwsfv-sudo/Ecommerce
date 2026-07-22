package com.ecommerce.quality.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ecommerce.quality.model.UserBehaviorEvent;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

class JsonKafkaRecordSerializationSchemaTest {
    @Test
    void serializesStandardEventToConfiguredTopic() {
        JsonKafkaRecordSerializationSchema<UserBehaviorEvent> schema =
                new JsonKafkaRecordSerializationSchema<>("clean-topic");
        UserBehaviorEvent event = new UserBehaviorEvent("evt-1", "u-1", "p-1", "view",
                "2026-07-22T10:00:00Z", "app", "android", "home");

        ProducerRecord<byte[], byte[]> record = schema.serialize(event, null, 123L);

        assertEquals("clean-topic", record.topic());
        assertTrue(new String(record.value(), StandardCharsets.UTF_8).contains("\"event_id\":\"evt-1\""));
    }
}
