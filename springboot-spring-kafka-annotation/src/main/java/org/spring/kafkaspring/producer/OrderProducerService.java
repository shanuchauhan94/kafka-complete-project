package org.spring.kafkaspring.producer;

import org.spring.kafkaspring.model.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class OrderProducerService {

    private final KafkaTemplate<String, Order> kafkaTemplate;

    @Autowired
    public OrderProducerService(KafkaTemplate<String, Order> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendUserDate(String store) {
        kafkaTemplate.send("user-topic", store, new Order(UUID.randomUUID().toString(), "Apple IPhone", 199, "Apple Store"));
    }

}
