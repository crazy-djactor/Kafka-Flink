package com.flink;


import com.flink.model.Backup;
import com.flink.model.InputMessage;
import com.flink.operator.BackupAggregator;
import com.flink.operator.InputMessageTimestampAssigner;
import com.flink.operator.WordsCapitalizer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import static com.flink.connector.Consumers.*;
import static com.flink.connector.Producers.*;

public class FlinkDataPipeline {
    static String TOPIC_IN = "Topic1-IN";
    static String TOPIC_OUT = "Topic3-OUT";
    static String BOOTSTRAP_SERVER = "localhost:9092";
    static int Data_Length = 3000;
    static int step = 1;
    static int Pattern_Length = 10;
    static int Forecast_horizon = 5;
    static float Precision = 0.95f;

    public static void capitalize() throws Exception {
        String inputTopic = "flink_input";
        String outputTopic = "flink_output";
        String consumerGroup = "baeldung";
        String address = "localhost:9092";

        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer<String> flinkKafkaConsumer =
                createStringConsumerForTopic(inputTopic, address, consumerGroup);
        flinkKafkaConsumer.setStartFromEarliest();

        DataStream<String> stringInputStream =
                environment.addSource(flinkKafkaConsumer);

        FlinkKafkaProducer<String> flinkKafkaProducer =
                createStringProducer(outputTopic, address);

        stringInputStream
                .map(new WordsCapitalizer())
                .addSink(flinkKafkaProducer);

        environment.execute();
    }

    public static void createBackup () throws Exception {
        String inputTopic = "flink_input";
        String outputTopic = "flink_output";
        String consumerGroup = "baeldung";
        String kafkaAddress = "localhost:9092";

        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer<InputMessage> flinkKafkaConsumer =
                createInputMessageConsumer(inputTopic, kafkaAddress, consumerGroup);
        flinkKafkaConsumer.setStartFromEarliest();

        flinkKafkaConsumer
                .assignTimestampsAndWatermarks(new InputMessageTimestampAssigner());
        FlinkKafkaProducer<Backup> flinkKafkaProducer =
                createBackupProducer(outputTopic, kafkaAddress);

        DataStream<InputMessage> inputMessagesStream =
                environment.addSource(flinkKafkaConsumer);

        inputMessagesStream
                .countWindowAll(Data_Length)
                .aggregate(new BackupAggregator())
                .addSink(flinkKafkaProducer);

        FlinkKafkaProducer<String> inputProducer =
                createStringProducer(inputTopic, kafkaAddress);

        environment.execute();
    }

    public static void main(String[] args) throws Exception {
        createBackup();
    }

}
