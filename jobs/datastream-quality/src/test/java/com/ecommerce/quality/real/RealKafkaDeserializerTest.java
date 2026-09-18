package com.ecommerce.quality.real;

import static org.junit.jupiter.api.Assertions.*;
import java.util.ArrayList;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

class RealKafkaDeserializerTest {
    @Test
    void retainsSourceCoordinatesIncludingTombstonesAsPojo() throws Exception {
        var decoder = new RealKafkaDeserializer();
        var rows = new ArrayList<KafkaEnvelope>();
        Collector<KafkaEnvelope> collector = new Collector<>() {
            public void collect(KafkaEnvelope value) { rows.add(value); }
            public void close() {}
        };
        decoder.deserialize(new ConsumerRecord<byte[], byte[]>("real_behavior_events_v1", 3, 87L, null, null), collector);
        assertEquals(1, rows.size());
        assertEquals(3, rows.get(0).partition);
        assertEquals(87L, rows.get(0).offset);
        assertEquals("real_behavior_events_v1", rows.get(0).topic);
        assertNull(rows.get(0).payload);
        assertInstanceOf(PojoTypeInfo.class, decoder.getProducedType());
    }
}
