package com.kafka;

import com.kafka.connector.Producer;
import com.kafka.model.KafkaRecord;
import com.kafka.operator.Aggregator;
import com.kafka.schema.DeserializeSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import com.forecast.ForecastConfig;

public class FlinkDataPipeline {

    @SuppressWarnings("serial")
    public static void StartPipeLine() throws Exception {
        Producer<String> p = new Producer<String>(ForecastConfig.BOOTSTRAP_SERVER, StringSerializer.class.getName());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // to use allowed lateness
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties props = new Properties();
        props.put("bootstrap.servers", ForecastConfig.BOOTSTRAP_SERVER);
        props.put("client.id", "flink-example3");

        // consumer to get both key/values per Topic
        FlinkKafkaConsumer<KafkaRecord> kafkaConsumer = new FlinkKafkaConsumer<>(ForecastConfig.TOPIC_IN,
                new DeserializeSchema(), props);

        // for allowing Flink to handle late elements
        kafkaConsumer.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<KafkaRecord>()
        {
            @Override
            public long extractAscendingTimestamp(KafkaRecord record)
            {
                return record.timestamp;
            }
        });

        kafkaConsumer.setStartFromLatest();

        // Create Kafka producer from Flink API
        Properties prodProps = new Properties();
        prodProps.put("bootstrap.servers", ForecastConfig.BOOTSTRAP_SERVER);

        FlinkKafkaProducer<String> kafkaProducer =
                new FlinkKafkaProducer<String>(ForecastConfig.TOPIC_OUT,
                        ((value, timestamp) -> new ProducerRecord<byte[], byte[]>(ForecastConfig.TOPIC_OUT,
                                "myKey".getBytes(), value.getBytes())),
                        prodProps,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        // create a stream to ingest data from Kafka with key/value
        DataStream<KafkaRecord> stream = env.addSource(kafkaConsumer);

        stream.filter((record) -> record.value != null && !record.value.isEmpty())
            .keyBy(record -> record.key)
            .countWindow(ForecastConfig.Data_Length, ForecastConfig.step)
//            .allowedLateness(Time.milliseconds(500))
            .aggregate(new Aggregator())
            .addSink(kafkaProducer);

        // produce a number as string every second
        new TestGenerator(p, ForecastConfig.TOPIC_IN).start();

        // for visual topology of the pipeline. Paste the below output in https://flink.apache.org/visualizer/
        System.out.println( env.getExecutionPlan() );

        // start flink
        env.execute();
    }

}
