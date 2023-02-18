package com.spring.kafka.transaction;

import com.spring.kafka.transaction.consumer.OrderConsumer;
import com.spring.kafka.transaction.producer.OrderProducer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.concurrent.ExecutionException;

@SpringBootApplication
public class SpringbootKafkaTransactionApplication {

	public static void main(String[] args) throws ExecutionException, InterruptedException {
	//	SpringApplication.run(SpringbootKafkaTransactionApplication.class, args);
		OrderProducer.publishMessage();
		OrderConsumer.consume();
	}

}
