package com.forecast;

public class ForecastConfig {
    static public int Data_Length = 3000;
    static public int step = 1;
    static public int Pattern_Length = 10;
    static public int Forecast_horizon = 5;
    static public float Precision = 0.95f;

    static public String TOPIC_IN = "Topic1-IN";
    static public String TOPIC_OUT = "Topic3-OUT";
    static public String BOOTSTRAP_SERVER = "localhost:9092";

}
