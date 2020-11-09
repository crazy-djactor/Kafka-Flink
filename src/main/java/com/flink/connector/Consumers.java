package com.flink.connector;

import com.flink.model.InputMessage;
import com.flink.schema.InputMessageDeserializationSchema;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Consumers {

    public static FlinkKafkaConsumer<String> createStringConsumerForTopic(
            String topic, String kafkaAddress, String kafkaGroup) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        props.setProperty("group.id", kafkaGroup);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
    }

    public static FlinkKafkaConsumer<InputMessage> createInputMessageConsumer(String topic, String kafkaAddress, String kafkaGroup) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaAddress);
        properties.setProperty("group.id", kafkaGroup);
        return new FlinkKafkaConsumer<InputMessage>(
                topic, new InputMessageDeserializationSchema(), properties);
    }
}
