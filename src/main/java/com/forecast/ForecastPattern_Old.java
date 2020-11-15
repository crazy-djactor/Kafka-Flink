package com.forecast;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class ForecastPattern_Old {
    static List<String> Data = null;
    public static String Forecast(List<String> l_Values, int l_PatternLength, int l_Horizon, float Precision) {
        int i;
        if (l_Values.size() <= 0) {
            return "No Input Data";
        }
        if (l_PatternLength < 2) {
            return "Pattern Length must be bigger than 1";
        }
        if (l_Horizon < 1) {
            return "Horizon must be bigger than 0";
        }
        if (Precision < 0 || Precision > 1) {
            return "Precision needs to be between 0 and 1";
        }
        if (Data == null) {
            Data = new ArrayList<String>(l_Values);
        }
        else {
            int n_OriginSize = Data.size();
            if (n_OriginSize < l_Values.size()) {
                Data = new ArrayList<String>(l_Values);
            } else {
                for (i = 0; i < n_OriginSize - l_Values.size(); i++) {
                    Data.set(i, Data.get(i + l_Values.size()));
                }
                for (i = n_OriginSize - l_Values.size(); i < n_OriginSize; i++) {
                    Data.set(i, l_Values.get(i - n_OriginSize + l_Values.size()));
                }
            }
        }
        int UBoundData = Data.size() - 1;
        String[] arrayData = Data.toArray(new String[UBoundData + 1]);

        float[] Pattern;
        Pattern = new float[(int)l_PatternLength];
        int UBoundPattern = Pattern.length - 1;

        for (i = 0; i < l_PatternLength; i ++) {
            Pattern[i] = Float.parseFloat(arrayData[UBoundData - UBoundPattern + i]);
        }

        float AvgChgPositive = 0.0f, AvgChgNegative = 0.0f;
        int NoOfNoChange = 0, NoOfPositives = 0, NoOfNegatives = 0, NoOfAll;

        for (i = 0; i <= UBoundData - l_PatternLength - l_Horizon; i ++) {
            float[] DataPart = new float[UBoundPattern + 1];
            for (int i2 = 0; i2 <= UBoundPattern; i2 ++) {
                DataPart[i2] = Float.parseFloat(arrayData[i + i2]);
            }
            double Correlation = PearsonCorrelation(DataPart, Pattern);
            if (Correlation >= Precision) {
                if (Float.parseFloat(arrayData[i + UBoundPattern + l_Horizon]) > Float.parseFloat(arrayData[i + UBoundPattern])) {
                    NoOfPositives = NoOfPositives + 1;
                    AvgChgPositive = AvgChgPositive +
                            ((Float.parseFloat(arrayData[i + UBoundPattern + l_Horizon])/Float.parseFloat(arrayData[i + UBoundPattern])) - 1) * 100;
                }
                else if (Float.parseFloat(arrayData[i + UBoundPattern + l_Horizon]) < Float.parseFloat(arrayData[i + UBoundPattern])) {
                    NoOfNegatives = NoOfNegatives + 1;
                    AvgChgNegative = AvgChgNegative +
                            (1 - Float.parseFloat(arrayData[i + UBoundPattern + l_Horizon])/Float.parseFloat(arrayData[i + UBoundPattern])) * 100;
                }
                else {
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
            return "";
        return String.format("%d; %s; %s; %s; %s", NoOfAll,
                df2.format((float)(NoOfPositives)/NoOfAll*100),
                df2.format((float)(NoOfNegatives)/NoOfAll*100),
                df4.format(AvgChgPositive),
                df4.format(AvgChgNegative));
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
