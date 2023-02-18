package com.spring.kafka.avro.producer;

import com.spring.kafka.avro.Order;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.File;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class OrderAvroProducer {

    public static void publishMessage() throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("schema.registry.url", "http://localhost:8081");
        // message will go into partition 2 only for this produce
        Long timeStamp = new Date().getTime();
        Order value = new Order(UUID.randomUUID().toString(), "Apple MackBook Pro", 100, "Apple Store");
        String key = (String) value.getOwnerName();
        String topic = "avro-topic";
        ProducerRecord<String, Order> record = new ProducerRecord<>(topic, 2, timeStamp, key, value);

        try (KafkaProducer<String, Order> producer = new KafkaProducer<>(props)) {
            RecordMetadata recordMetadata = producer.send(record).get();
            System.err.println("Producer send Records into topic " +
                    recordMetadata.topic() + " partition " +
                    recordMetadata.partition() + " Offset " +
                    recordMetadata.offset());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void publishGenericMessage() throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("schema.registry.url", "http://localhost:8081");
        // message will go into partition 2 only for this produce

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {

            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(new File("src/main/resources/schema/order.avsc"));
            GenericRecord order = new GenericData.Record(schema);
            order.put("id", UUID.randomUUID().toString());
            order.put("productName", "IPhone");
            order.put("quantity", 100);
            order.put("ownerName", "Shanu Chauhan");
            String key = (String) order.get("ownerName");
            String topic = "avro-topic";
            Long timeStamp = new Date().getTime();
            ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, 2, timeStamp, key, order);
            RecordMetadata recordMetadata = producer.send(record).get();
            System.err.println("Producer send Records into topic " +
                    recordMetadata.topic() + " partition " +
                    recordMetadata.partition() + " Offset " +
                    recordMetadata.offset());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
