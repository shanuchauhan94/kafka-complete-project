package com.spring.kafka.transaction.producer;

import com.spring.kafka.transaction.model.Order;
import com.spring.kafka.transaction.partition.CustomPartition;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class OrderProducer {

    public static void publishMessage() throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomSerializer.class.getName());
        props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartition.class.getName());
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "order-trans-" + UUID.randomUUID()); // each trans trans id must be unique

        Order order1 = new Order(UUID.randomUUID().toString(), "Samsung ChromeBook", 100, "Samsung Store");
        Order order2 = new Order(UUID.randomUUID().toString(), "Apple MackBook Pro", 200, "Apple Store");

        KafkaProducer<String, Order> producer = new KafkaProducer<>(props);
        producer.initTransactions();
        ProducerRecord<String, Order> record1 = new ProducerRecord<>("order-transaction-topic", order1.getOwnerName(), order1);
        ProducerRecord<String, Order> record2 = new ProducerRecord<>("order-transaction-topic", order2.getOwnerName(), order2);

        try {
            producer.beginTransaction();
            producer.send(record1); // no need to call callback method b/c in any exception it rollback all transaction
            producer.send(record2);
            producer.commitTransaction();
        } catch (Exception e) {
            producer.abortTransaction();
            e.printStackTrace();
        } finally {
            producer.close();
        }

    }
}
