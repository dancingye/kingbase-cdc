package com.nnrz.util;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyKafkaUtil {

    private static final String DEFAULT_BOOTSTRAP = "192.168.2.188:9092";

    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP);
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1");

        return new FlinkKafkaProducer<>(
                topic,
                (KafkaSerializationSchema<String>) (s, aLong) -> new ProducerRecord<>(topic, s.getBytes()),
                props,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );
    }
}