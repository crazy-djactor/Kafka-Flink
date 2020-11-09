package com.forecast.connector.model;

import com.fasterxml.jackson.annotation.JsonProperty;


import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

public class ForecastRecord {

    @JsonProperty("inputMessages")
    List<KafkaRecord> inputMessages;
    @JsonProperty("backupTimestamp")
    LocalDateTime backupTimestamp;
    @JsonProperty("uuid")
    UUID uuid;

    public ForecastRecord(List<KafkaRecord> inputMessages, LocalDateTime backupTimestamp) {
        this.inputMessages = inputMessages;
        this.backupTimestamp = backupTimestamp;
        this.uuid = UUID.randomUUID();
        float [] valueArray = new float[inputMessages.size()];
        for(int i = 0; i < inputMessages.size(); i ++) {
            KafkaRecord record = inputMessages.get(i);
            valueArray[i] = Float.parseFloat(record.value);
        }

    }

    public List<KafkaRecord> getInputMessages() {
        return inputMessages;
    }
}