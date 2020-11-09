package com.forecast.connector.operator;

import com.forecast.connector.model.KafkaRecord;
import com.forecast.connector.model.ForecastRecord;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;


public class Aggregator implements AggregateFunction<KafkaRecord, List<KafkaRecord>, ForecastRecord> {
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
    public ForecastRecord getResult(List<KafkaRecord> inputMessages) {
        return new ForecastRecord(inputMessages, LocalDateTime.now());
    }

    @Override
    public List<KafkaRecord> merge(List<KafkaRecord> inputMessages, List<KafkaRecord> acc1) {
        inputMessages.addAll(acc1);
        return inputMessages;
    }
}