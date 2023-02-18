package org.spring.kafkaspring.utility;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.spring.kafkaspring.model.Order;

import java.io.IOException;

public class CustomDeserializer implements Deserializer {


    @Override
    public Object deserialize(String topic, byte[] bytes) {

        System.err.println("deserialize Topic Name :: " + topic);
        Order order;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            order = objectMapper.readValue(bytes, Order.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return order;
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        return CustomDeserializer.this.deserialize(topic, data);
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
