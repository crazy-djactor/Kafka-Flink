package com.forecast;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
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
        String inputFile = dir + "/data.txt";
        List<String> l_ArrAll = new ArrayList<String>();
        try (Stream<String> lines = Files.lines(Paths.get(inputFile))) {
            lines.forEach(l_ArrAll::add);
        }
        catch(IOException e){
            System.out.println(e);
        }
        String l_Result = null;
        for(int i = 0; i < l_ArrAll.size() - Data_Length; i = i + Step) {
            List<String> l_ArrPart = new ArrayList<String>();
            for(int i2 = 0; i2 < Data_Length; i2 = i2 + 1) {
                l_ArrPart.add(l_ArrAll.get(i + i2));
            }
            l_Result = ForecastPattern.Forecast(l_ArrPart, Pattern_Length, Forecast_horizon, Precision);
            System.out.println(l_Result);
        }
    }
}
