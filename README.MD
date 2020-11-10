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

The main classes are packed into com.kafka package. Connector, model, operator, schema are for consuming and producing kafka topics and flink jobs.

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

The core of this stream is setting sliding window for Data_Length.
Datastream is overlapped and Data_Length number of values are used in Forecast at a time.
In Aggregator, Data_Length of values in the sliding window at a time should be passed into Forecast method.
Returned result will be published by .addSink(kafkaProducer)