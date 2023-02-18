package com.kafka.consumer.group;

import com.kafka.consumer.group.consumer.OrderConsumer;
import com.kafka.consumer.group.producer.OrderProducer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.concurrent.ExecutionException;

@SpringBootApplication
public class SpringBootKafkaConsumerGroupApplication {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
       // SpringApplication.run(SpringBootKafkaConsumerGroupApplication.class, args);
        OrderProducer.publishMessage();
        OrderConsumer.consume();
    }

}
