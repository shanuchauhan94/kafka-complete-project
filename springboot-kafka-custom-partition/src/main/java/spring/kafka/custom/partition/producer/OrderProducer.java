package spring.kafka.custom.partition.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import spring.kafka.custom.partition.model.Order;
import spring.kafka.custom.partition.partition.CustomPartition;

import java.util.Date;
import java.util.Properties;
import java.util.UUID;

public class OrderProducer {

    public static void publishMessage() {

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomSerializer.class.getName());
        props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartition.class.getName());


        Long timeStamp = new Date().getTime();
        Order value = new Order(UUID.randomUUID().toString(), "Samsung ChromeBook Pro", 200, "Samsung Store");
        String key = value.getOwnerName();
        // message will go into partition 2 only for this produce
        // ProducerRecord<String, Order> record = new ProducerRecord<>("custom-partition-topic", 2, timeStamp, key, value);
        ProducerRecord<String, Order> record = new ProducerRecord<>("custom-partition-value-topic", key, value);

        try (KafkaProducer<String, Order> producer = new KafkaProducer<>(props)) {
            RecordMetadata recordMetadata = producer.send(record).get();
            System.err.println("Producer send Records into topic ::  partition " + recordMetadata.partition());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
