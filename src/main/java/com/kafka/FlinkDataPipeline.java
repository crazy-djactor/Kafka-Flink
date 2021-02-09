package com.kafka;

import com.kafka.connector.Producer;
import com.kafka.model.ForecastRecord;
import com.kafka.model.KafkaRecord;
import com.kafka.operator.Aggregator;
import com.kafka.schema.DeserializeSchema;
import org.apache.flink.api.common.functions.FilterFunction;
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

    @SuppressWarnings("serial")
    public static void StartPipeLine() throws Exception {
        Producer<String> p = new Producer<String>(ForecastConfig.BOOTSTRAP_SERVER, StringSerializer.class.getName());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // produce a number as string every second

        // to use allowed lateness
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties props = new Properties();
        props.put("bootstrap.servers", ForecastConfig.BOOTSTRAP_SERVER);
        props.put("client.id", "flink-example3");

        // consumer to get both key/values per Topic
        System.out.println("In Topic: " + ForecastConfig.TOPIC_IN);

        FlinkKafkaConsumer<KafkaRecord> kafkaConsumer = new FlinkKafkaConsumer<>(ForecastConfig.TOPIC_IN,
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
        Properties prodProps = new Properties();
        prodProps.put("bootstrap.servers", ForecastConfig.BOOTSTRAP_SERVER);

        System.out.println("Out Topic: " + ForecastConfig.TOPIC_OUT);

        FlinkKafkaProducer<ForecastRecord> kafkaProducer =
                new FlinkKafkaProducer<>(ForecastConfig.TOPIC_OUT,
                        (new KafkaSerializationSchema<ForecastRecord>() {
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(ForecastRecord element, @Nullable Long timestamp) {
                                String key = element.getKey();
                                String value = element.getValue();
                                return new ProducerRecord<byte[], byte[]>(
                                                ForecastConfig.TOPIC_OUT, key.getBytes(), value.getBytes());
                            }
                        }),
                        prodProps,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        // create a stream to ingest data from Kafka with key/value
        DataStream<KafkaRecord> stream = env.addSource(kafkaConsumer);
        stream.filter((record) -> record.value != null && !record.value.isEmpty())
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

        if (!ForecastConfig.Test.equals("")) {
            new TestGenerator(p, ForecastConfig.TOPIC_IN, ForecastConfig.Test).start();
        }

        // start flink
        env.execute();
    }

}
