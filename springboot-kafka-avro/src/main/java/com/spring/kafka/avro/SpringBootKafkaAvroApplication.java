package com.spring.kafka.avro;

import com.spring.kafka.avro.consumer.OrderAvroConsumer;
import com.spring.kafka.avro.producer.OrderAvroProducer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.concurrent.ExecutionException;

@SpringBootApplication
public class SpringBootKafkaAvroApplication {

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		// SpringApplication.run(SpringBootKafkaAvroApplication.class, args);
		OrderAvroProducer.publishMessage();
		OrderAvroConsumer.consume();
	}

}
