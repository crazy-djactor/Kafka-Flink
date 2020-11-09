package com.forecast.connector;

public class TestGenerator extends Thread
{
    int counter = 0;
    final Producer<String> p;
    final String topic;

    public TestGenerator(Producer<String> p, String topic)
    {
        this.p = p;
        this.topic = topic;
    }

    @Override
    public void run()
    {
        try
        {
            while( ++counter > 0 )
            {
                p.send(topic, "[" + counter + "]");

                Thread.sleep( 500 );
            }
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
}