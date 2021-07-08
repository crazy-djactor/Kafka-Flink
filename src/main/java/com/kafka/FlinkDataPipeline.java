package com.kafka;

import com.kafka.connector.Producer;
import com.kafka.model.ForecastRecord;
import com.kafka.model.KafkaRecord;
import com.kafka.operator.Aggregator;
import com.kafka.schema.DeserializeSchema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import com.forecast.ForecastConfig;

import javax.annotation.Nullable;

public class FlinkDataPipeline {

    public static void StartPipeLine() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // produce a number as string every second

        // to use allowed lateness
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final Properties props = new Properties();
        props.put("bootstrap.servers", ForecastConfig.BOOTSTRAP_SERVER);
        props.put("client.id", "flink-start-pipeline");
        props.put("group.id", "flink-start-pipeline0");
        props.put("topic.name.in", ForecastConfig.TOPIC_IN);
        props.put("topic.name.out", ForecastConfig.TOPIC_OUT);
        // consumer to get both key/values per Topic
        System.out.println("In Topic: " + ForecastConfig.TOPIC_IN);


        FlinkKafkaConsumer<KafkaRecord> kafkaConsumer = new FlinkKafkaConsumer<>(props.getProperty("topic.name.in"),
                new DeserializeSchema(), props);
        // for allowing Flink to handle late elements
        kafkaConsumer.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<KafkaRecord>()
        {
            @Override
            public long extractAscendingTimestamp(KafkaRecord record)
            {
                if (record.timestamp == null) {
                    return 0;
                }
                return record.timestamp;
            }
        });

        kafkaConsumer.setStartFromLatest();

        // Create Kafka producer from Flink API
        System.out.println("Out Topic: " + ForecastConfig.TOPIC_OUT);

        FlinkKafkaProducer<ForecastRecord> kafkaProducer =
                new FlinkKafkaProducer<>(ForecastConfig.TOPIC_OUT,
                        (new KafkaSerializationSchema<ForecastRecord>() {
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(ForecastRecord element, @Nullable Long timestamp) {
                                String key = element.getKey();
                                String value = element.getValue();
                                return new ProducerRecord<byte[], byte[]>(
                                        props.getProperty("topic.name.out"), key.getBytes(), value.getBytes());
                            }

                        }),
                        props,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        // create a stream to ingest data from Kafka with key/value
        DataStream<KafkaRecord> stream = env.addSource(kafkaConsumer);
        stream.filter(new FilterFunction<KafkaRecord>() {
            @Override
            public boolean filter(KafkaRecord record) throws Exception {
                return record.value != null && !record.value.isEmpty();
            }
        })
            .keyBy(new KeySelector<KafkaRecord, String>() {
                @Override
                public String getKey(KafkaRecord kfRecord){
                    return kfRecord.key;
                }
            })
            .countWindow(ForecastConfig.Data_Length, ForecastConfig.step)
            .aggregate(new Aggregator())
            .filter(new FilterFunction<ForecastRecord>() {
                @Override
                public boolean filter(ForecastRecord resultRecord) throws Exception {
                    return resultRecord.getKey() != null &&
                            ! resultRecord.getKey().isEmpty() &&
                            resultRecord.getValue() != null && !resultRecord.getValue().isEmpty();
                }
            })
            .addSink(kafkaProducer);


        // for visual topology of the pipeline. Paste the below output in https://flink.apache.org/visualizer/
        System.out.println( env.getExecutionPlan() );
//        ForecastConfig.Test = "/home/djactor/Documents/data_JSON.txt";
//        if (!ForecastConfig.Test.equals("")) {
//            Producer<String> outputProducer = new Producer<String>(ForecastConfig.BOOTSTRAP_SERVER, StringSerializer.class.getName());
//            new TestGenerator(outputProducer, props.getProperty("topic.name.in"), ForecastConfig.Test).start();
//        }

        // start flink
        env.execute();
    }
    public static void TestPipeLine() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // produce a number as string every second

        // to use allowed lateness
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final Properties props = new Properties();
        props.put("bootstrap.servers", ForecastConfig.BOOTSTRAP_SERVER);
        props.put("client.id", "flink-start-pipeline");
        props.put("group.id", "flink-start-pipeline");
        props.put("topic.name.in", ForecastConfig.TOPIC_IN);
        props.put("topic.name.out", ForecastConfig.TOPIC_OUT);
        // consumer to get both key/values per Topic
        System.out.println("In Topic: " + ForecastConfig.TOPIC_IN);


        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(props.getProperty("topic.name.in"),
                new SimpleStringSchema(), props);
        // for allowing Flink to handle late elements
        kafkaConsumer.setStartFromGroupOffsets();

        // Create Kafka producer from Flink API
        System.out.println("Out Topic: " + ForecastConfig.TOPIC_OUT);

        FlinkKafkaProducer<ForecastRecord> kafkaProducer =
                new FlinkKafkaProducer<>(ForecastConfig.TOPIC_OUT,
                        (new KafkaSerializationSchema<ForecastRecord>() {
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(ForecastRecord element, @Nullable Long timestamp) {
                                String key = element.getKey();
                                String value = element.getValue();
                                return new ProducerRecord<byte[], byte[]>(
                                        props.getProperty("topic.name.out"), key.getBytes(), value.getBytes());
                            }

                        }),
                        props,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        // create a stream to ingest data from Kafka with key/value
        DataStream<String> stream = env.addSource(kafkaConsumer);
        stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String record) throws Exception {
                return record != null && !record.isEmpty();
            }
        });
        // for visual topology of the pipeline. Paste the below output in https://flink.apache.org/visualizer/
        System.out.println( env.getExecutionPlan() );
//        ForecastConfig.Test = "/home/djactor/Documents/data_JSON.txt";
//        if (!ForecastConfig.Test.equals("")) {
//            Producer<String> outputProducer = new Producer<String>(ForecastConfig.BOOTSTRAP_SERVER, StringSerializer.class.getName());
//            new TestGenerator(outputProducer, props.getProperty("topic.name.in"), ForecastConfig.Test).start();
//        }

        // start flink
        env.execute();
    }
}
