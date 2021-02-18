package com.test;

import com.forecast.ForecastConfig;
import org.apache.commons.cli.*;
import com.kafka.KafkaStreamsLiveTest;

public class Main {
    public static void main(String[] args) {
//        Options options = new Options();
//        Option input = new Option("i", "input", true, "The Kafka topic name for input");
//        input.setRequired(true);
//        options.addOption(input);
//
//        Option bootstrap = new Option("bst", "bootstrap-server", true, "Bootstrap server for kafka, default value is localhost:9092");
//        bootstrap.setRequired(true);
//        options.addOption(bootstrap);
//
//        Option test = new Option("test", "test", true, "Test data for test running, ex: C:/data_JSON.txt");
//        test.setRequired(true);
//        options.addOption(test);
//
//        CommandLineParser parser = new DefaultParser();
//        HelpFormatter formatter = new HelpFormatter();
//        CommandLine cmd;
//
//        try {
//            cmd = parser.parse(options, args);
//            String inputTopic = cmd.getOptionValue("input");
//            String BootstrapServer = cmd.getOptionValue("bootstrap-server");
//            String Test = cmd.getOptionValue("test");
//
//            ForecastConfig.TOPIC_IN = inputTopic;
//            System.out.println(ForecastConfig.TOPIC_IN);
//
//            if (BootstrapServer != null) {
//                ForecastConfig.BOOTSTRAP_SERVER = BootstrapServer;
//            }
//
//            if (Test != null) {
//                ForecastConfig.Test = Test;
//            }
//
//            String outputString = String.format("Test String Generate from Topic %s",
//                    ForecastConfig.TOPIC_IN);
//            System.out.println(outputString);
//            KafkaStreamsLiveTest.shouldTestKafkaStreams();
//        } catch (ParseException | InterruptedException e) {
//            System.out.println(e.getMessage());
//            formatter.printHelp("utility-name", options);
//            System.exit(1);
//        }
    }

}
