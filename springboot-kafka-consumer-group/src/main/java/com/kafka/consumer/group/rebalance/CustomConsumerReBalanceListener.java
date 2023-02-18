package com.kafka.consumer.group.rebalance;

import com.kafka.consumer.group.model.Order;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;

public class CustomConsumerReBalanceListener implements ConsumerRebalanceListener {

    private final KafkaConsumer<String, Order> consumer;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets;

    public CustomConsumerReBalanceListener(KafkaConsumer<String, Order> consumer, Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        this.consumer = consumer;
        this.currentOffsets = currentOffsets;
    }


    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        consumer.commitSync(currentOffsets);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {

    }
}
