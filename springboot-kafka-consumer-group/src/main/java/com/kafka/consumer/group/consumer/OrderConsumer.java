package com.kafka.consumer.group.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.consumer.group.model.Order;
import com.kafka.consumer.group.rebalance.CustomConsumerReBalanceListener;
import com.kafka.consumer.group.serializers.CustomDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class OrderConsumer {

    public static void consume() {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "customConsumerGroup");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // latest, earliest, none
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "10246789");
        props.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "200"); // default 500 ms
        props.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
        //props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "3000"); // tells broker to consumer's timeout
        props.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "12"); // default 1MB
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "order-consumer-client");
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        // default RangeAssignor.class.getName() assigned 2 consecutive partition to consumer
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        ObjectMapper objectMapper = new ObjectMapper();

        try (KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props)) {

            // consumer.subscribe(Collections.singleton("order-consumer-group-topic");
            consumer.subscribe(Collections.singleton("order-consumer-group-topic"), new CustomConsumerReBalanceListener(consumer, currentOffsets));
            ConsumerRecords<String, Order> consumerRecords = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, Order> record : consumerRecords) {
                Map<String, String> orderMap = new HashMap<>();
                Order data = record.value();
                data.setPartition(record.partition());
                orderMap.put(record.key(), objectMapper.writeValueAsString(data));
                System.err.println(orderMap);
                // offset commit to minimize the re-balance risks
                currentOffsets = Collections.singletonMap(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1));
                consumer.commitAsync(currentOffsets, // always start new offset
                        new OffsetCommitCallback() {
                            @Override
                            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offSets, Exception e) {
                                if (e != null) {
                                    System.err.println("failed for offSets : " + offSets);
                                    e.printStackTrace();
                                }
                            }
                        });
                System.err.println("commitAsync called : ");
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
