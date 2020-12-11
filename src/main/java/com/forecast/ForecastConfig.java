package com.forecast;

public class ForecastConfig {
    static public int Data_Length = 200;
    static public int step = 1;
    static public int Pattern_Length = 10;
    static public int Forecast_horizon = 5;
    static public float Precision = 0.7f;

    static public String TOPIC_IN = "Topic1-IN";
    static public String TOPIC_OUT = "Topic3-OUT";
    static public String BOOTSTRAP_SERVER = "192.168.2.169:9092";
//    static public String TOPIC_IN = "inSpring";
//    static public String TOPIC_OUT = "outSpring";
//    static public String BOOTSTRAP_SERVER = "45.10.26.123:39092";
    static public String Test = "";
}
