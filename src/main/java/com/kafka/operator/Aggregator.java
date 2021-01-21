package com.kafka.operator;

import com.kafka.model.KafkaRecord;
import com.kafka.model.ForecastRecord;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.time.LocalDateTime;
import java.util.*;



//public class Aggregator implements AggregateFunction<KafkaRecord, List<KafkaRecord>, String> {
//    static Map<String, CircularFifoQueue<KafkaRecord>> map_records = new HashMap<String, CircularFifoQueue<KafkaRecord>>();
//
//    @Override
//    public List<KafkaRecord> createAccumulator() {
//        return new ArrayList<KafkaRecord>();
//    }
//
//    @Override
//    public List<KafkaRecord> add(KafkaRecord inputMessage, List<KafkaRecord> inputMessages) {
//        inputMessages.add(inputMessage);
//        return inputMessages;
//    }
//
//    @Override
//    public String getResult(List<KafkaRecord> inputMessages) {
//        KafkaRecord last_add = inputMessages.get(inputMessages.size() - 1);
//        JSONObject jsonObject = new JSONObject(last_add.value);
//        String key = (String) jsonObject.get("StockID");
//        CircularFifoQueue<KafkaRecord> queue;
//        if (map_records.containsKey(key)) {
//            queue = map_records.get(key);
//            queue.add(last_add);
//        } else {
//            queue = new CircularFifoQueue<>(ForecastConfig.Data_Length);
//            queue.add(last_add);
//            map_records.put(key, queue);
//        }
//
//        if (queue.isAtFullCapacity()) {
////            KafkaRecord [] array = (KafkaRecord[]) (queue.toArray());
////            List<KafkaRecord> inputData = Arrays.asList(array.clone());
//            List<KafkaRecord> inputData = new ArrayList<>(queue);
//            ForecastRecord result = new ForecastRecord(inputData, LocalDateTime.now());
//            System.out.println(result.getValue());
//            return result.getValue();
//        }
//        return "";
//    }
//
//    @Override
//    public List<KafkaRecord> merge(List<KafkaRecord> inputMessages, List<KafkaRecord> acc1) {
//        inputMessages.addAll(acc1);
//        return inputMessages;
//    }
//}

public class Aggregator implements AggregateFunction<KafkaRecord, List<KafkaRecord>, ForecastRecord> {

    @Override
    public List<KafkaRecord> createAccumulator() {
        return new ArrayList<KafkaRecord>();
    }

    @Override
    public List<KafkaRecord> add(KafkaRecord inputMessage, List<KafkaRecord> inputMessages) {
        inputMessages.add(inputMessage);
        return inputMessages;
    }

    @Override
    public ForecastRecord getResult(List<KafkaRecord> inputMessages) {
        List<KafkaRecord> inputData = new ArrayList<>(inputMessages);
        return new ForecastRecord(inputData, LocalDateTime.now());
    }

    @Override
    public List<KafkaRecord> merge(List<KafkaRecord> inputMessages, List<KafkaRecord> acc1) {
        inputMessages.addAll(acc1);
        return inputMessages;
    }
}