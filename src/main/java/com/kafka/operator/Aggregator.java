package com.kafka.operator;

import com.forecast.ForecastConfig;
import com.forecast.ForecastPattern;
import com.kafka.model.KafkaRecord;
import com.kafka.model.ForecastRecord;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Aggregator implements AggregateFunction<KafkaRecord, List<KafkaRecord>, String> {
//    static Map<String, List<KafkaRecord>> map_records = new HashMap<String, List<KafkaRecord>>();

    @Override
    public List<KafkaRecord> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<KafkaRecord> add(KafkaRecord inputMessage, List<KafkaRecord> inputMessages) {
//        System.out.println(inputMessage.key + " " + inputMessage.value);
        inputMessages.add(inputMessage);
        return inputMessages;
    }

    @Override
    public String getResult(List<KafkaRecord> inputMessages) {
//        String current_key = inputMessages.get(0).key;
//        int newSize = 0;
//        List<KafkaRecord> lst = new ArrayList<KafkaRecord>();
//            lst = inputMessages;
//            if (lst.get(0).key.equals("ForexAFNNoExpiry")) {
//                System.out.println("===========");
//                for (int i = 0; i < lst.size(); i ++) {
//                    System.out.println(lst.get(i));
//                }
//                System.out.println("===========");
//            }
        if (inputMessages.size() >= ForecastConfig.Data_Length){
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