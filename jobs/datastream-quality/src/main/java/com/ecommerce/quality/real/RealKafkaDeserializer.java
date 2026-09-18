package com.ecommerce.quality.real;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.util.Collector;

public final class RealKafkaDeserializer implements KafkaRecordDeserializationSchema<KafkaEnvelope> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<KafkaEnvelope> output) {
        output.collect(new KafkaEnvelope(record.topic(), record.partition(), record.offset(), record.value()));
    }

    @Override
    public TypeInformation<KafkaEnvelope> getProducedType() { return TypeInformation.of(KafkaEnvelope.class); }
}
