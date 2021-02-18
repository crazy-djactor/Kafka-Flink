package com.kafka;

import com.kafka.connector.Producer;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class TestGenerator extends Thread {
    int counter = 0;
    Producer<String> p;
    final String topic;
    final String test;

    public TestGenerator(Producer<String> p, String topic, String test) {
        this.p = p;
        this.topic = topic;
        this.test = test;
    }

    @Override
    public void run() {
        System.out.println("input dir = " + this.test);
        List<String> l_ArrAll = new ArrayList<String>();
        try (Stream<String> lines = Files.lines(Paths.get(this.test))) {
            lines.forEach(l_ArrAll::add);
        } catch (IOException e) {
            System.out.println(e);
        }
        try {
            Thread.sleep(5000);

            for (String s : l_ArrAll) {
                JSONObject jsonObject = new JSONObject(s);
                String stockID = (String)jsonObject.get("StockID");
                if (stockID.equals("")) {
                    continue;
                }
                p.send(this.topic, s, stockID);
//                p.send(topic, s, "forecast");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}