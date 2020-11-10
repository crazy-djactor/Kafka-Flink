package com.kafka;

import com.kafka.connector.Producer;

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

    public TestGenerator(Producer<String> p, String topic) {
        this.p = p;
        this.topic = topic;
    }

    @Override
    public void run() {

        final String dir = System.getProperty("user.dir");
        System.out.println("current dir = " + dir);
        String inputFile = dir + "/data.txt";
        List<String> l_ArrAll = new ArrayList<String>();
        try (Stream<String> lines = Files.lines(Paths.get(inputFile))) {
            lines.forEach(l_ArrAll::add);
        } catch (IOException e) {
            System.out.println(e);
        }
        try {
            Thread.sleep(5000);

            for (String s : l_ArrAll) {
                p.send(topic, s);
                Thread.sleep(1);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//            int counter = 0;
//            while (++ counter > 0) {
//                p.send(topic, "[" + counter + "]");
//
//                try {
//                    Thread.sleep( 5);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
    }
}