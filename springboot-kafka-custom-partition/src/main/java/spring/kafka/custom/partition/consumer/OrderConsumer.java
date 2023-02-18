package spring.kafka.custom.partition.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import spring.kafka.custom.partition.model.Order;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class OrderConsumer {

    public static void consume() {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", CustomDeserializer.class.getName());
        props.setProperty("group.id", "custom-partition-Group");
        props.setProperty("auto.offset.reset", "latest");

        ObjectMapper objectMapper = new ObjectMapper();

        try (KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props)) { // will close consumer automatically
           // Collection<TopicPartition> partitionsInfo1 = hardCodedPartitionKeyToConsumeOnlyDataFrom(consumer);
            Collection<TopicPartition> partitionsInfo = genericPartitionKeyToConsumeOnlyDataFrom(consumer);
            consumer.assign(partitionsInfo);

            ConsumerRecords<String, Order> consumerRecords = consumer.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<String, Order> record : consumerRecords) {
                Map<String, String> orderMap = new HashMap<>();
                Order data = record.value();
                data.setPartition(record.partition());
                orderMap.put(record.key(), objectMapper.writeValueAsString(data));
                System.err.println(orderMap);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static Collection<TopicPartition> genericPartitionKeyToConsumeOnlyDataFrom(KafkaConsumer<String, Order> consumer) {

        List<PartitionInfo> partitionsInfo = consumer.partitionsFor("custom-partition-value-topic");
        return partitionsInfo.stream()
                .filter(f -> f.partition() == 1)
                .map(m -> new TopicPartition("custom-partition-value-topic", m.partition()))
                .collect(Collectors.toList());
    }

    private static Collection<TopicPartition> hardCodedPartitionKeyToConsumeOnlyDataFrom(KafkaConsumer<String, Order> consumer) {
        Collection<TopicPartition> partitions = new ArrayList<>();
        //  partitions.add(new TopicPartition("custom-partition-topic", 0));
        partitions.add(new TopicPartition("custom-partition-value-topic", 1));
        return partitions;
    }
}
