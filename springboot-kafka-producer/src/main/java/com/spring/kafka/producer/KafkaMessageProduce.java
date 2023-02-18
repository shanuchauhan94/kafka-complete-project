package com.spring.kafka.producer;

import com.spring.kafka.callback.ProducerCallBack;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Component
public class KafkaMessageProduce {
    public Map<String, Object> sendMessage(String key, String value) {

        Map<String, Object> map = new HashMap<>();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        KafkaProducer<String, Integer> producer = new KafkaProducer<>(props);
        // message will go into partition 2 only for this produce
        Long timeStamp = new Date().getTime();
        ProducerRecord<String, Integer> record = new ProducerRecord<>("my-topic", 2, timeStamp, key, Integer.parseInt(value));
        try {
            asynchronousCall(producer, record);
            // synchronousCall(map, producer, record);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
        return map;
    }

    private void asynchronousCall(KafkaProducer<String, Integer> producer, ProducerRecord<String, Integer> record) {
        producer.send(record, new ProducerCallBack());
        System.err.println("message sent.");

    }

    private static Map<String, Object> synchronousCall(Map<String, Object> map, KafkaProducer<String, Integer> producer, ProducerRecord<String, Integer> record) throws InterruptedException, ExecutionException {
        // Sync call
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata metadata = future.get();
        System.err.println("Message send Successfully. ");
        map.put("Event Type ", "Synchronous");
        map.put("Topic Name ", metadata.topic());
        map.put("Partition Number ", metadata.partition());
        map.put("Offset Name ", metadata.offset());
        map.put("Message send Synchronously ", true);
        return map;
    }
}
