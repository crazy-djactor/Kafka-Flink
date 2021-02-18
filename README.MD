## Apache Kafka - Flink datastream

The data from one kafka topic will be processed and published into another kafka stream.
For streaming, apache Flink is used.

- Apache Kafka installation

  https://kafka.apache.org/quickstart
- Create kafka topics.

  - bin/kafka-topics.sh --create --topic input_topic --bootstrap-server localhost:9092
  - bin/kafka-topics.sh --create --topic output_topic --bootstrap-server localhost:9092
  
- Project environment
  - Kafka and flink are using Maven.
  
  https://www.jetbrains.com/help/idea/work-with-maven-dependencies.html#generate_maven_dependency

- Structure

  The config in com.forcat.ForecastConfig is used for configuring kafka server and topics.
    
    
    public class ForecastConfig {
        static public int Data_Length = 3000;
        static public int step = 1;
        static public int Pattern_Length = 10;
        static public int Forecast_horizon = 5;
        static public float Precision = 0.95f;
    
        static public String TOPIC_IN = "Topic1-IN";
        static public String TOPIC_OUT = "Topic3-OUT";
        static public String BOOTSTRAP_SERVER = "localhost:9092";
    
    }
    
The project entry point is com.kafka.Main

The main classes are packed into com.kafka package. 

Connector, model, operator, schema are for consuming and producing kafka topics and flink jobs.

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
            .aggregate(new Aggregator())
            .addSink(kafkaProducer);

        // produce a number as string every second
        new TestGenerator(p, ForecastConfig.TOPIC_IN).start();

        // for visual topology of the pipeline. Paste the below output in https://flink.apache.org/visualizer/
        System.out.println( env.getExecutionPlan() );

        // start flink
        env.execute();
    }
- Run
  https://ci.apache.org/projects/flink/flink-docs-release-1.11/try-flink/local_installation.html
  
The core of this stream is setting sliding window for Data_Length.

Datastream is overlapped and Data_Length number of values are used in Forecast at a time.

In Aggregator, Data_Length of values in the sliding window from Topic_In at a time should be passed into Forecast method.

Returned result will be published by .addSink(kafkaProducer) to Topic_OUT.


======================================

### Example Run configuration

This configuration is processing json inputs from Topic1-IN and process, write outputs to Topic3-OUT.

1) Run zookeeper, kafka server and create topics for input, output (In the kafka installation directory)
 
  - bin/zookeeper-server-start.sh config/zookeeper.properties
  - bin/kafka-server-start.sh config/server.properties
  - bin/kafka-topics.sh --create --topic Topic1-IN --bootstrap-server localhost:9092
  - bin/kafka-topics.sh --create --topic Topic3-OUT --bootstrap-server localhost:9092
  
2) Start flink cluster (In the flink binary directory) 
  - ./bin/start-cluster.sh
  - ./bin/flink run /WORK/Java/DataConvesion.jar -i Topic1-IN -o Topic3-OUT -bst localhost:9092 -test /WORK/Java/data_JSON.txt
    
    Params are here in details.
    
    
     -bst,--bootstrap-server <arg>   Bootstrap server for kafka, default value
                                     is localhost:9092
                                     
     -fh,--forcast_horizon <arg>     Horizon value for forecasting, default
                                     value is 5
                                     
     -i,--input <arg>                The Kafka topic name for input
     
     -length,--data_length <arg>     DataLength for forecasting, default value
                                     is 3000
                                     
     -o,--output <arg>               The Kafka topic name for output
     
     -pl,--pattern_length <arg>      Pattern Length for forecasting, default
                                     value is 10
                                     
     -pr,--precision <arg>           Precision value for forecasting, default
                                     value is 0.95f
                                     
     -step,--step <arg>              Step value for forecasting, default value
                                     is 1
                                     
     -test,--test <arg>              Test data for test running, ex:
                                     C:/data_JSON.txt


 To check output for kafka topic, let's run console consumer of output topic.
  
  - bin/kafka-console-consumer.sh --topic Topic3-OUT --from-beginning --bootstrap-server localhost:9092


curl -X POST -H "Content-Type: application/json" --data '
{
    "programArgsList": [
      "--input",
      "Topic1-IN",
      "--output",
      "Topic3-OUT",
      "--data_length",
      "200",
      "--step",
      "1",
      "--pattern_length",
      "5",
      "--forcast_horizon",
      "200",
      "--precision",
      "0.7",
      "--bootstrap-server",
      "45.10.26.123:19092"
    ]
}
' http://localhost:8081/jars/9c31c199-5f28-4c69-af7e-06b0cd393ea7_DataConvesion.jar/run

curl -X POST -H "Content-Type: application/json" --data '
{
"programArgsList": [
  "--input",
  "Topic1-IN",
  "--output",
  "Topic3-OUT",
  "--data_length",
  "200",
  "--step",
  "1",
  "--pattern_length",
  "5",
  "--forcast_horizon",
  "200",
  "--precision",
  "0.7",
  "--bootstrap-server",
  "localhost:9092"
  ]
}
' http://localhost:8081/jars/bd48a505-4a32-456a-b2d5-691e4b5938dc_DataConvesion.jar/run
