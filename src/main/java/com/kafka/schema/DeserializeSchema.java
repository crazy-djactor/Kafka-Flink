package com.kafka.schema;

import com.kafka.model.KafkaRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONObject;

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
            JSONObject newRecord = new JSONObject(new String(record.value()));
            data.value = new String(record.value());
            data.timestamp = record.timestamp();
            data.key = newRecord.getString("StockID");
        } catch (Exception e){
        }

        return data;
    }

    @Override
    public TypeInformation<KafkaRecord> getProducedType() {
        return TypeInformation.of(KafkaRecord.class);
    }
}
