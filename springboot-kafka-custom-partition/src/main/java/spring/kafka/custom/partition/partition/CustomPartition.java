package spring.kafka.custom.partition.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class CustomPartition implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyByte, Object value, byte[] valueByte, Cluster cluster) {

        List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
        if (((String) key).equals("Apple Store")) {
            return 2;
        } else if (((String) key).equals("Samsung Store")) {
            return 1;
        }
        return Math.abs(Utils.murmur2(keyByte) % partitions.size() - 1);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
