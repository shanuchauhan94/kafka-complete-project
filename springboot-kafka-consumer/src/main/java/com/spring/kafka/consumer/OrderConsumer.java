package com.spring.kafka.consumer;


import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {

    public static void poolMessages() {

        System.err.println("********************** Order - Consumer ********************");

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.setProperty("group.id", "orderGroup"); // any name

        try (KafkaConsumer<String, Number> consumer = new KafkaConsumer<>(props)) { // will close consumer automatically
            consumer.subscribe(Collections.singleton("my-topic"));
            ConsumerRecords<String, Number> consumeData = consumer.poll(Duration.ofSeconds(3));
            consumeData.forEach(data -> {
                System.err.println("Product Name " + data.key());
                System.err.println("Quantity " + data.value());
            });
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
