package com.spring.kafka.serializer;

import com.spring.kafka.model.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class OrderProducer {

    public static void publishMessage() throws ExecutionException, InterruptedException {

        Map<String, Object> map = new HashMap<>();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "com.spring.kafka.serializer.CustomSerializer");

        // message will go into partition 2 only for this produce
        Long timeStamp = new Date().getTime();
        Order value = new Order(UUID.randomUUID().toString(), "Apple MackBook Pro", 100);
        ProducerRecord<String, Order> record = new ProducerRecord<>("my-topic", 2, timeStamp, "Apple Store", value);

        try (KafkaProducer<String, Order> producer = new KafkaProducer<>(props)) {
            RecordMetadata recordMetadata = producer.send(record).get();
            System.err.println("Producer send Records into topic " +
                    recordMetadata.topic() + " partition " +
                    recordMetadata.partition() + " Offset " +
                    recordMetadata.offset());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
