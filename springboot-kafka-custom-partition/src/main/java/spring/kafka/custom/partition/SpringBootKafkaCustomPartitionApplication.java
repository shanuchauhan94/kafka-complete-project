package spring.kafka.custom.partition;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import spring.kafka.custom.partition.consumer.OrderConsumer;
import spring.kafka.custom.partition.producer.OrderProducer;

import java.util.concurrent.ExecutionException;

@SpringBootApplication
public class SpringBootKafkaCustomPartitionApplication {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // SpringApplication.run(SpringBootKafkaCustomPartitionApplication.class, args);
        OrderProducer.publishMessage();
        OrderConsumer.consume();
    }

}
