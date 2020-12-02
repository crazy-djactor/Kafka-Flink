package com.kafka;

import java.time.LocalTime;
import java.util.Properties;

import com.forecast.ForecastConfig;
import com.kafka.connector.Producer;
import com.kafka.model.KafkaRecord;
import com.kafka.operator.Aggregator;
import com.kafka.schema.DeserializeSchema;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import org.apache.commons.cli.*;

import static com.kafka.FlinkDataPipeline.StartPipeLine;


public class Main
{
    static int Data_Length;
    static int step;
    static int Pattern_Length;
    static int Forecast_horizon;
    static float Precision;
    static String BOOTSTRAP_SERVER = "localhost:9092";

    @SuppressWarnings("serial")
    public static void Test1() throws Exception {
        String TOPIC_IN = "Topic1-IN";
        String TOPIC_OUT = "Topic3-OUT";


        Producer<String> p = new Producer<String>(BOOTSTRAP_SERVER, StringSerializer.class.getName());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put("client.id", "flink-example1");

        // Reading data directly as <Key, Value> from Kafka. Write an inner class containing key, value
        // and use it to deserialise Kafka record.
        // Reference => https://stackoverflow.com/questions/53324676/how-to-use-flinkkafkaconsumer-to-parse-key-separately-k-v-instead-of-t
        FlinkKafkaConsumer<KafkaRecord> kafkaConsumer = new FlinkKafkaConsumer<>(TOPIC_IN, new DeserializeSchema(), props);

        Properties prodProps = new Properties();
        prodProps.put("bootstrap.servers", BOOTSTRAP_SERVER);
        FlinkKafkaProducer<String> kafkaProducer =
                new FlinkKafkaProducer<String>(ForecastConfig.TOPIC_OUT,
                        ((value, timestamp) -> new ProducerRecord<byte[], byte[]>(ForecastConfig.TOPIC_OUT,
                                "myKey".getBytes(), value.getBytes())),
                        prodProps,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        kafkaConsumer.setStartFromEarliest();

        Data_Length = 5;
        step = 1;
        Pattern_Length = 10;
        Forecast_horizon = 5;
        Precision = 0.95f;

        // create a stream to ingest data from Kafka as a custom class with explicit key/value
        DataStream<KafkaRecord> stream = env.addSource(kafkaConsumer);

        // supports timewindow without group by key
        stream
                .countWindowAll(10, 1)
                .aggregate(new Aggregator())
                .addSink(kafkaProducer);
//                .reduce(new ReduceFunction<KafkaRecord>()
//                {
//                    KafkaRecord result = new KafkaRecord();
//
//                    @Override
//                    public KafkaRecord reduce(KafkaRecord record1, KafkaRecord record2) throws Exception
//                    {
//                        System.out.println(LocalTime.now() + " -> " + record1 + "   " + record2);
//                        result.key = record1.key;
//                        result.value = record1.value + " " + record2.value;
//                        return result;
//                    }
//                }).addSink(kafkaProducer); // immediate printing to console


        //.keyBy( (KeySelector<KafkaRecord, String>) KafkaRecord::getKey )
        //.timeWindow(Time.seconds(5))

        // produce a number as string every second
        new TestGenerator(p, TOPIC_IN, "/media/djactor/d0e07426-1a67-4ab9-937f-f4da35c51c14/WORK/Java/Kafka-Flink/data_JSON.txt").start();

        // for visual topology of the pipeline. Paste the below output in https://flink.apache.org/visualizer/
        System.out.println( env.getExecutionPlan() );

        // start flink
        env.execute();
    }

    @SuppressWarnings("serial")
    public static void Test2( String[] args ) throws Exception
    {
        String TOPIC_IN = "Topic2-IN";
        String TOPIC_OUT = "Topic2-OUT";
        String BOOTSTRAP_SERVER = "localhost:9092";


        Producer<String> p = new Producer<String>(BOOTSTRAP_SERVER, StringSerializer.class.getName());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put("client.id", "flink-example2");

        // Alternate consumer to get only values per Topic
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(TOPIC_IN, new SimpleStringSchema(), props);

        kafkaConsumer.setStartFromLatest();

        // Create Kafka producer from Flink API
        Properties prodProps = new Properties();
        prodProps.put("bootstrap.servers", BOOTSTRAP_SERVER);

        FlinkKafkaProducer<String> kafkaProducer =
                new FlinkKafkaProducer<String>(TOPIC_OUT,
                        ((value, timestamp) -> new ProducerRecord<byte[], byte[]>(TOPIC_OUT, "myKey".getBytes(), value.getBytes())),
                        prodProps,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        // create a stream to ingest data from Kafka with value as String
        DataStream<String> stream = env.addSource(kafkaConsumer);

        stream
                .timeWindowAll(Time.seconds(5)) // ignoring grouping per key
                .reduce(new ReduceFunction<String>()
                {
                    @Override
                    public String reduce(String value1, String value2) throws Exception
                    {
                        System.out.println(LocalTime.now() + " -> " + value1 + "   " + value2);
                        return value1+value2;
                    }
                })
                .addSink(kafkaProducer);

        // produce a number as string every second
//        new TestGenerator(p, TOPIC_IN, "/media/djactor/d0e07426-1a67-4ab9-937f-f4da35c51c14/WORK/Java/Kafka-Flink/data_JSON.txt").start();

        // for visual topology of the pipeline. Paste the below output in https://flink.apache.org/visualizer/
        System.out.println( env.getExecutionPlan() );

        // start flink
        env.execute();
    }

    @SuppressWarnings("serial")
    public static void Test3() throws Exception {
        String TOPIC_IN = "Topic3-IN";
        String TOPIC_OUT = "Topic3-OUT";

        Producer<String> p = new Producer<String>(BOOTSTRAP_SERVER, StringSerializer.class.getName());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // to use allowed lateness
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put("client.id", "flink-example3");

        // consumer to get both key/values per Topic
        FlinkKafkaConsumer<KafkaRecord> kafkaConsumer = new FlinkKafkaConsumer<>(TOPIC_IN, new DeserializeSchema(), props);

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
        prodProps.put("bootstrap.servers", BOOTSTRAP_SERVER);

        FlinkKafkaProducer<String> kafkaProducer =
                new FlinkKafkaProducer<String>(TOPIC_OUT,
                        ((value, timestamp) -> new ProducerRecord<byte[], byte[]>(TOPIC_OUT, "myKey".getBytes(), value.getBytes())),
                        prodProps,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        // create a stream to ingest data from Kafka with key/value
        DataStream<KafkaRecord> stream = env.addSource(kafkaConsumer);

        stream
                .filter((record) -> record.value != null && !record.value.isEmpty())
                .keyBy(record -> record.key)
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.milliseconds(500))
                .aggregate(new AggregateFunction<KafkaRecord, String, String>()  // kafka aggregate API is very simple but same can be achieved by Flink's reduce
                {
                    @Override
                    public String createAccumulator() {
                        return "";
                    }

                    @Override
                    public String add(KafkaRecord record, String accumulator) {
                        return accumulator + record.value.length();
                    }

                    @Override
                    public String getResult(String accumulator) {
                        return accumulator;
                    }

                    @Override
                    public String merge(String a, String b) {
                        return a+b;
                    }
                })
                .addSink(kafkaProducer);

        // produce a number as string every second
        new TestGenerator(p, TOPIC_IN, "/media/djactor/d0e07426-1a67-4ab9-937f-f4da35c51c14/WORK/Java/Kafka-Flink/data_JSON.txt").start();

        // for visual topology of the pipeline. Paste the below output in https://flink.apache.org/visualizer/
        System.out.println( env.getExecutionPlan() );

        // start flink
        env.execute();
    }

    public static void main( String[] args ) throws Exception {
        ForecastConfig.Test = "/media/djactor/d0e07426-1a67-4ab9-937f-f4da35c51c14/WORK/Java/DataConvesion/data_JSON.txt";
        StartPipeLine();

        Options options = new Options();
        Option input = new Option("i", "input", true, "The Kafka topic name for input");
        input.setRequired(true);
        options.addOption(input);
        Option output = new Option("o", "output", true, "The Kafka topic name for output");
        output.setRequired(true);
        options.addOption(output);

        Option data_length = new Option("length", "data_length", true, "DataLength for forecasting, default value is 3000");
        data_length.setRequired(false);
        options.addOption(data_length);
        Option data_out_length = new Option("output_length", "output_data_length", true, "DataLength for Output stream, default value is 3000");
        data_out_length.setRequired(false);
        options.addOption(data_out_length);

        Option step = new Option("step", "step", true, "Step value for forecasting, default value is 1");
        step.setRequired(false);
        options.addOption(step);
        Option patternLength = new Option("pl", "pattern_length", true, "Pattern Length for forecasting, default value is 10");
        data_length.setRequired(false);
        options.addOption(patternLength);
        Option forecast = new Option("fh", "forcast_horizon", true, "Horizon value for forecasting, default value is 5");
        forecast.setRequired(false);
        options.addOption(forecast);
        Option precision = new Option("pr", "precision", true, "Precision value for forecasting, default value is 0.95f");
        forecast.setRequired(false);
        options.addOption(precision);

        Option bootstrap = new Option("bst", "bootstrap-server", true, "Bootstrap server for kafka, default value is localhost:9092");
        forecast.setRequired(false);
        options.addOption(bootstrap);

        Option test = new Option("test", "test", true, "Test data for test running, ex: C:/data_JSON.txt");
        forecast.setRequired(false);
        options.addOption(test);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
            String inputTopic = cmd.getOptionValue("input");
            String outputTopic = cmd.getOptionValue("output");
            String DataLength = cmd.getOptionValue("data_length");
            String OutDataLength = cmd.getOptionValue("output_data_length");
            String Step = cmd.getOptionValue("step");

            String Precision = cmd.getOptionValue("precision");
            String PatternLength = cmd.getOptionValue("pattern_length");
            String Forecast_Horizon = cmd.getOptionValue("forcast_horizon");
            String BootstrapServer = cmd.getOptionValue("bootstrap-server");
            String Test = cmd.getOptionValue("test");

            ForecastConfig.TOPIC_IN = inputTopic;
            ForecastConfig.TOPIC_OUT = outputTopic;

            if (DataLength != null) {
                ForecastConfig.Data_Length = Integer.parseInt(DataLength);
            }
            if (OutDataLength != null) {
                ForecastConfig.OutDataLength = Integer.parseInt(OutDataLength);
            }

            if (Forecast_Horizon != null) {
                ForecastConfig.Forecast_horizon = Integer.parseInt(Forecast_Horizon);
            }

            if (PatternLength != null) {
                ForecastConfig.Pattern_Length = Integer.parseInt(PatternLength);
            }

            if (Step != null) {
                ForecastConfig.step = Integer.parseInt(Step);
            }

            if (Precision != null) {
                ForecastConfig.Precision = Float.parseFloat(Precision);
            }

            if (BootstrapServer != null) {
                ForecastConfig.BOOTSTRAP_SERVER = BootstrapServer;
            }

            if (Test != null) {
                ForecastConfig.Test = Test;
            }

            String outputString = String.format("Forecasting from Topic %s to %s with (DataLength: %d, Forecast_Horizon: %d, PatternLength: %d, Step: %d, Precision: %.2f",
                    inputTopic, outputTopic,
                    ForecastConfig.Data_Length,
                    ForecastConfig.Forecast_horizon,
                    ForecastConfig.Pattern_Length,
                    ForecastConfig.step,
                    ForecastConfig.Precision);
            System.out.println(outputString);
            StartPipeLine();
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
        }

    }
}