package com.spring.kafka.transaction.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spring.kafka.transaction.model.Order;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class OrderConsumer {

    public static void consume() {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", CustomDeserializer.class.getName());
        props.setProperty("group.id", "customPartitionGroup"); // any name

        ObjectMapper objectMapper = new ObjectMapper();

        try (KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props)) { // will close consumer automatically
            consumer.subscribe(Collections.singleton("order-transaction-topic"), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    consumer.seekToBeginning(partitions);
                    partitions.forEach(consumer::position); // call position as seekToBeginning evaluates lazily
                    consumer.commitAsync();
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> collection) {

                }
            });
            ConsumerRecords<String, Order> consumerRecords = consumer.poll(Duration.ofSeconds(3));
            for (ConsumerRecord<String, Order> record : consumerRecords) {
                Map<String, String> orderMap = new HashMap<>();
                Order data = record.value();
                data.setPartition(record.partition());
                orderMap.put(record.key(), objectMapper.writeValueAsString(data));
                System.err.println("Partition No:: " + record.partition());
                System.err.println(orderMap);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
