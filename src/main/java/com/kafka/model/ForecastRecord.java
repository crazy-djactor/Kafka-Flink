package com.kafka.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import com.forecast.ForecastConfig;
import com.forecast.ForecastPattern;
import org.json.JSONException;
import org.json.JSONObject;

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
        String stockID = "";
        for (KafkaRecord record : inputMessages) {
            try {
                JSONObject jsonObject = new JSONObject(record.value);
                String Price = (String)jsonObject.get("price");
                if (Price.equals("")) {
                    continue;
                }
                valueArray.add(Price);
                stockID = record.key;
            } catch (JSONException ignored){
            }
        }
        
        this.value = stockID + " " + ForecastPattern.CoreForest(valueArray,
                ForecastConfig.Pattern_Length,
                ForecastConfig.Forecast_horizon,
                ForecastConfig.Precision);

//        this.value = ForecastPattern.Forecast(valueArray, ForecastConfig.Pattern_Length,
//                ForecastConfig.Forecast_horizon, ForecastConfig.Precision);
    }

    public List<KafkaRecord> getInputMessages() {
        return inputMessages;
    }
    public String getValue() {return value; }
}