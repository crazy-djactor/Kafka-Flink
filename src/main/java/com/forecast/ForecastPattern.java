package com.forecast;

import org.json.JSONException;
import org.json.JSONObject;
import java.text.DecimalFormat;
import java.util.*;

public class ForecastPattern {
    static List<String> static_Data = null;
    static long HistorySize = 0;

    public static JSONObject Forecast(String l_Value, long l_HistoryLength, long l_PatternLength, long l_Horizon, double Precision) {
        int i;
        String Price = null;
        String StockId = null;
        if (l_Value.equals("")) {
//            "No Input Data";
            return null;
        }
        if (l_PatternLength < 2) {
//            "Pattern Length must be bigger than 1";
            return null;
        }
        if (l_Horizon < 1) {
//            "Horizon must be bigger than 0";
            return null;
        }
        if (Precision < 0 || Precision > 1) {
//            "Precision needs to be between 0 and 1";
            return null;
        }

        if (HistorySize == 0) {
            if (l_HistoryLength <= 0) {
//                 "HistoryLength must be bigger than 0";
                return null;
            } else {
                HistorySize = l_HistoryLength;
            }
        }

        try {
            JSONObject jsonObject = new JSONObject(l_Value);
            Price = (String)jsonObject.get("price");
            if (Price.equals("")) {
//                "No price data";
                return null;
            }
            StockId = (String)jsonObject.get("StockID");
            if (StockId.equals("")) {
//                "No Stock ID";
                return null;
            }
        } catch (JSONException err){
//            err.getMessage();
            return null;
        }

        if (static_Data == null) {
            static_Data = new ArrayList<String>();
            static_Data.add(Price);
        }
        else {
            int last_index = static_Data.size();
            if (static_Data.size() < HistorySize) {
                static_Data.add(Price);
            }
            else {
                for (i = 0; i < last_index - 1; i++)
                    static_Data.set(i, static_Data.get(i + 1));
                static_Data.set(last_index - 1, Price);
            }
        }
        return CoreForest(static_Data, l_PatternLength, l_Horizon, Precision);
    }

    public static JSONObject CoreForest(List<String> Data, long l_PatternLength, long l_Horizon, double Precision) {
        int UBoundData = Data.size() - 1;
        int i;
        String[] arrayData = Data.toArray(new String[UBoundData + 1]);

        float[] Pattern;
        Pattern = new float[(int)l_PatternLength];
        int UBoundPattern = Pattern.length - 1;
        if (! arrayData[0].equals("") && UBoundData > UBoundPattern) {
            for (i = 0; i < l_PatternLength; i++) {
                Pattern[i] = Float.parseFloat(arrayData[UBoundData - UBoundPattern + i]);
            }

            float AvgChgPositive = 0.0f, AvgChgNegative = 0.0f;
            int NoOfNoChange = 0, NoOfPositives = 0, NoOfNegatives = 0, NoOfAll;

            for (i = 0; i <= UBoundData - l_PatternLength - l_Horizon; i++) {
                float[] DataPart = new float[UBoundPattern + 1];
                for (int i2 = 0; i2 <= UBoundPattern; i2++) {
                    DataPart[i2] = Float.parseFloat(arrayData[i + i2]);
                }
                double Correlation = PearsonCorrelation(DataPart, Pattern);
                if (Correlation >= Precision) {
                    if (Float.parseFloat(arrayData[(int) (i + UBoundPattern + l_Horizon)]) > Float.parseFloat(arrayData[i + UBoundPattern])) {
                        NoOfPositives = NoOfPositives + 1;
                        AvgChgPositive = AvgChgPositive +
                                ((Float.parseFloat(arrayData[(int) (i + UBoundPattern + l_Horizon)]) / Float.parseFloat(arrayData[i + UBoundPattern])) - 1) * 100;
                    } else if (Float.parseFloat(arrayData[(int) (i + UBoundPattern + l_Horizon)]) < Float.parseFloat(arrayData[i + UBoundPattern])) {
                        NoOfNegatives = NoOfNegatives + 1;
                        AvgChgNegative = AvgChgNegative +
                                (1 - Float.parseFloat(arrayData[(int) (i + UBoundPattern + l_Horizon)]) / Float.parseFloat(arrayData[i + UBoundPattern])) * 100;
                    } else {
                        NoOfNoChange = NoOfNoChange + 1;
                    }
                }
            }

            if (NoOfPositives > 0) {
                AvgChgPositive = AvgChgPositive / NoOfPositives;
            }
            if (NoOfNegatives > 0) {
                AvgChgNegative = AvgChgNegative / NoOfNegatives;
            }
            NoOfAll = NoOfPositives + NoOfNegatives + NoOfNoChange;

            DecimalFormat df2 = new DecimalFormat("0.00");
            DecimalFormat df4 = new DecimalFormat("0.0000");
            if (NoOfAll == 0)
                return null;
            JSONObject res = new JSONObject();
//            "StockID": "ForexEURHUFNoExpiry",
//                    "NoOfPattern":"37","NoOfPos":"45.95","NoOfNeg":"54.95","AvgChgPos":"0.3373","AvgChgNeg":"0.1803"
            res.put("NoOfPattern", Integer.toString(NoOfAll));
            res.put("NoOfPos", df2.format(Float.valueOf((float)(NoOfPositives) / NoOfAll * 100)));
            res.put("NoOfNeg", df2.format(Float.valueOf((float) (NoOfNegatives) / NoOfAll * 100)));
            res.put("AvgChgPos", df4.format(Float.valueOf(AvgChgPositive)));
            res.put("AvgChgNeg", df4.format(Float.valueOf(AvgChgNegative)));
            return res;
//            return String.format("%d; %s; %s; %s; %s", NoOfAll,
//                    df2.format((float) (NoOfPositives) / NoOfAll * 100),
//                    df2.format((float) (NoOfNegatives) / NoOfAll * 100),
//                    df4.format(AvgChgPositive),
//                    df4.format(AvgChgNegative));
        }
        return null;
    }

    public static double PearsonCorrelation(float[] X, float[] Y) {
        int UBoundX = X.length;
        int i;
        float XMean = 0, YMean = 0;
        float T1, T2;
        double XV, YV, S, Result;
        if (UBoundX <= 0) {
            return 0.0f;
        }
        for (i = 0; i < UBoundX; i ++) {
            XMean += X[i];
            YMean += Y[i];
        }
        //Mean
        XMean = XMean / UBoundX;
        YMean = YMean / UBoundX;

        //Numerator and denominator 
        XV = YV = S = 0;
        for (i = 0; i < UBoundX; i ++) {
            T1 = X[i] - XMean;
            T2 = Y[i] - YMean;
            XV = XV + T1 * T1;
            YV = YV + T2 * T2;
            S = S + T1 * T2;
        }
        if (XV == 0 || YV == 0) {
            Result = 0;
        }
        else {
            Result = S / (Math.sqrt(XV) * Math.sqrt(YV));
        }
        return Result;
    }

}
