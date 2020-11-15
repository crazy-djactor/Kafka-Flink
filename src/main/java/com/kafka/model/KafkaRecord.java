package com.kafka.model;

import java.io.Serializable;

import lombok.Data;

@SuppressWarnings("serial")
@Data
public class KafkaRecord implements Serializable
{
    public String key;
    public String value;
    public Long timestamp;

    @Override
    public String toString()
    {
        return key+":"+value;
    }

}

