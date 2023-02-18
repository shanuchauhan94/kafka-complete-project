package com.spring.kafka.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spring.kafka.model.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class OrderConsumer {

    public static void consume() {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", CustomDeserializer.class.getName());

        props.setProperty("group.id", "orderGroup"); // any name
        ObjectMapper objectMapper = new ObjectMapper();

        try (KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props)) { // will close consumer automatically
            consumer.subscribe(Collections.singleton("my-topic"));
            ConsumerRecords<String, Order> consumeData = consumer.poll(Duration.ofSeconds(3));
            for (ConsumerRecord<String, Order> data : consumeData) {
                Map<String, String> orderMap = new HashMap<>();
                orderMap.put(data.key(), objectMapper.writeValueAsString(data.value()));
                System.err.println(orderMap);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
