package com.kafka;

import com.kafka.connector.Producer;
import com.kafka.model.KafkaRecord;
import com.kafka.schema.DeserializeSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Ignore;
import java.util.Properties;
import com.forecast.ForecastConfig;

public class KafkaStreamsLiveTest {

    @Ignore("it needs to have kafka broker running on local")
    public static void shouldTestKafkaStreams() throws InterruptedException {
        //given
        Producer<String> outProducer = new Producer<String>(ForecastConfig.BOOTSTRAP_SERVER, StringSerializer.class.getName());

        Properties props = new Properties();
        props.put("bootstrap.servers", ForecastConfig.BOOTSTRAP_SERVER);
        props.put("client.id", "testGenerator");

        // consumer to get both key/values per Topic
        System.out.println("In Topic: " + ForecastConfig.TOPIC_IN);
        FlinkKafkaConsumer<KafkaRecord> kafkaConsumer = new FlinkKafkaConsumer<>(ForecastConfig.TOPIC_IN,
                new DeserializeSchema(), props);

        kafkaConsumer.setStartFromLatest();

        if (!ForecastConfig.Test.equals("")) {
            new TestGenerator(outProducer, ForecastConfig.TOPIC_IN, ForecastConfig.Test).start();
        }
    }

}
