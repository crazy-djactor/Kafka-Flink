package com.forecast;

import com.kafka.model.ForecastRecord;
import com.kafka.model.KafkaRecord;
import org.json.JSONObject;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class TestForecast {
    int Data_Length;
    int Step;
    int Pattern_Length;
    int Forecast_horizon;
    float Precision;

    public TestForecast(int _Data_Length, int _step, int _Pattern_Length, int _Forecast_horizon, float _Precision) {
        Data_Length = _Data_Length;
        Step = _step;
        Pattern_Length = _Pattern_Length;
        Forecast_horizon = _Forecast_horizon;
        Precision = _Precision;
    }
    public void Test() {
        URL location = ForecastPattern.class.getProtectionDomain().getCodeSource().getLocation();
        System.out.println(location.getFile());

        final String dir = System.getProperty("user.dir");
        System.out.println("current dir = " + dir);
        String inputFile = dir + "/level1.txt";
        List<String> l_ArrAll = new ArrayList<String>();
        try (Stream<String> lines = Files.lines(Paths.get(inputFile))) {
            lines.forEach(l_ArrAll::add);
        }
        catch(IOException e){
            System.out.println(e);
        }
        String l_Result = null;
        String l_Value = null;
        Map<String, List<String>> map_records = new HashMap<String, List<String>>();
        List<String> lst = new ArrayList<String>();
        int newSize;
        for(int i = 0; i < l_ArrAll.size(); i = i + Step) {
            l_Value = l_ArrAll.get(i);

            JSONObject jsonObject = new JSONObject(l_Value);
            String stockID = (String)jsonObject.get("StockID");

            if (stockID.equals("")) {
                continue;
            }
            String Price = (String)jsonObject.get("price");
            if (Price.equals("")) {
                continue;
            }

            if (map_records.containsKey(stockID)) {
                lst = map_records.get(stockID);
                lst.add(Price);
            }
            else {
                lst = new ArrayList<String>();
                lst.add(Price);
                map_records.put(stockID, lst);
            }

            if (lst.size() > Data_Length) {
                newSize = lst.size();
                lst = lst.subList(newSize - ForecastConfig.Data_Length + 1, newSize);
            }
            newSize = lst.size();
//            if (newSize > ForecastConfig.Pattern_Length){
                String ret = ForecastPattern.CoreForest(lst,
                        ForecastConfig.Pattern_Length,
                        ForecastConfig.Forecast_horizon,
                        ForecastConfig.Precision);
                if (! ret.equals(""))
                {
                    System.out.println(stockID + " " + ret);
                }

//            }
        }
    }
}
