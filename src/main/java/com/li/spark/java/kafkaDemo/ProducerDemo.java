package com.li.spark.java.kafkaDemo;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("metadata.broker.list", "hadoop02:9092,hadoop03:9092,hadoop04:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        for (int i = 1001; i <= 1100; i++)
            producer.send(new KeyedMessage<String, String>("first", "first-msg" + i));
    }
}
