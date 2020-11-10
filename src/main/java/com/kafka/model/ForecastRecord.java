package com.kafka.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import com.forecast.ForecastConfig;
import com.forecast.ForecastPattern;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ForecastRecord {
    @JsonProperty("backupTimestamp")
    LocalDateTime backupTimestamp;
    @JsonProperty("uuid")
    UUID uuid;
    @JsonProperty("value")
    String value;

    List<KafkaRecord> inputMessages;
    public ForecastRecord(List<KafkaRecord> inputMessages, LocalDateTime backupTimestamp) {
        this.inputMessages = inputMessages;
        this.backupTimestamp = backupTimestamp;
        this.uuid = UUID.randomUUID();
        List<String> valueArray = new ArrayList<String>();

        for (KafkaRecord record : inputMessages) {
            valueArray.add(record.value);
        }
//        this.value = ForecastPattern.Forecast(valueArray, ForecastConfig.Pattern_Length,
//                ForecastConfig.Forecast_horizon, ForecastConfig.Precision);
        this.value = ForecastPattern.Forecast(valueArray, ForecastConfig.Pattern_Length,
                ForecastConfig.Forecast_horizon, ForecastConfig.Precision);
    }

    public List<KafkaRecord> getInputMessages() {
        return inputMessages;
    }
    public String getValue() {return value; }
}