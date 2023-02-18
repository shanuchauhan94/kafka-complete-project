package org.spring.kafka.stream;

import org.spring.kafka.stream.producer.WordCountProducer;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringBootKafkaStreamApplication {

    public static void main(String[] args) {
        //  StreamProducer.streamData();
        WordCountProducer.streamData();
    }

}
