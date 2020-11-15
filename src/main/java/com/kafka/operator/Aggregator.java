package com.kafka.operator;

import com.forecast.ForecastConfig;
import com.forecast.ForecastPattern;
import com.kafka.model.KafkaRecord;
import com.kafka.model.ForecastRecord;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;


public class Aggregator implements AggregateFunction<KafkaRecord, List<KafkaRecord>, String> {
    @Override
    public List<KafkaRecord> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<KafkaRecord> add(KafkaRecord inputMessage, List<KafkaRecord> inputMessages) {
        inputMessages.add(inputMessage);
        return inputMessages;
    }

    @Override
    public String getResult(List<KafkaRecord> inputMessages) {
        if (inputMessages.size() == ForecastConfig.Data_Length){
            ForecastRecord result = new ForecastRecord(inputMessages, LocalDateTime.now());
            System.out.println(result.getValue());
            return result.getValue();
        }
        return "";
    }

    @Override
    public List<KafkaRecord> merge(List<KafkaRecord> inputMessages, List<KafkaRecord> acc1) {
        inputMessages.addAll(acc1);
        return inputMessages;
    }
}