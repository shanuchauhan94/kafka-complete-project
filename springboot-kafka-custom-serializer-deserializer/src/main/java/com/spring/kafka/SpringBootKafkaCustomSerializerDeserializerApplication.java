package com.spring.kafka;

import com.spring.kafka.deserializer.OrderConsumer;
import com.spring.kafka.model.Order;
import com.spring.kafka.serializer.OrderProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
public class SpringBootKafkaCustomSerializerDeserializerApplication {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //SpringApplication.run(SpringBootKafkaCustomSerializerDeserializerApplication.class, args);
        OrderProducer.publishMessage();
        OrderConsumer.consume();

    }

}
