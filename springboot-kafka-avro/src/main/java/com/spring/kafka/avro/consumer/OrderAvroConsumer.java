package com.spring.kafka.avro.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spring.kafka.avro.Order;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class OrderAvroConsumer {

    public static void consume() {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("schema.registry.url", "http://localhost:8081");
        props.setProperty("group.id", "avro-order-Group"); // any name
        props.setProperty("specific.avro.reader", "true");
        ObjectMapper objectMapper = new ObjectMapper();

        try (KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props)) { // will close consumer automatically
            consumer.subscribe(Collections.singleton("my-topic"));
            ConsumerRecords<String, Order> consumeData = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, Order> data : consumeData) {
                Map<String, String> orderMap = new HashMap<>();
                orderMap.put(data.key(), objectMapper.writeValueAsString(data.value()));
                System.err.println(orderMap);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void genericConsume() {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("schema.registry.url", "http://localhost:8081");
        props.setProperty("group.id", "avro-order-Group"); // any name
        props.setProperty("specific.avro.reader", "true");
        ObjectMapper objectMapper = new ObjectMapper();

        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) { // will close consumer automatically
            consumer.subscribe(Collections.singleton("my-topic"));
            ConsumerRecords<String, GenericRecord> consumeData = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, GenericRecord> data : consumeData) {
                Map<String, String> orderMap = new HashMap<>();
                orderMap.put(data.key(), objectMapper.writeValueAsString(data.value()));
                System.err.println(orderMap);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
