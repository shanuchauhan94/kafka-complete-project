package org.spring.kafka.stream.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.spring.kafka.stream.model.Order;

import java.nio.charset.StandardCharsets;

public class CustomSerializer implements Serializer<Order> {

    @Override
    public byte[] serialize(String topic, Order order) {
        System.err.println("serialize topic Name ********* " + topic);
        byte[] response;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            response = objectMapper.writeValueAsString(order).getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return response;
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Order order) {
        return CustomSerializer.this.serialize(topic, order);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
