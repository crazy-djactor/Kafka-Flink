package com.kafka.schema;

import com.kafka.model.KafkaRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@SuppressWarnings("serial")
public class DeserializeSchema implements KafkaDeserializationSchema<KafkaRecord>
{
    @Override
    public boolean isEndOfStream(KafkaRecord nextElement) {
        return false;
    }

    @Override
    public KafkaRecord deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        KafkaRecord data = new KafkaRecord();
        try {
            data.key =  new String(record.key());
            data.value = new String(record.value());
            System.out.println("deserialize == " + data.key + " " + data.value);

            data.timestamp = record.timestamp();
        } catch (Exception e){
        }

        return data;
    }

    @Override
    public TypeInformation<KafkaRecord> getProducedType() {
        return TypeInformation.of(KafkaRecord.class);
    }
}
