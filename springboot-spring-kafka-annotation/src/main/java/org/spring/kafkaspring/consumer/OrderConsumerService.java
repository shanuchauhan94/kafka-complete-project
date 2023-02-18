package org.spring.kafkaspring.consumer;

import org.spring.kafkaspring.model.Order;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumerService {

    @KafkaListener(topics = {"user-topic"})
    public void consumeData(Order order) {
        System.err.println("User age is :: " + order);
    }
}
