package org.spring.kafka.stream.producer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamProducer {

    public static void streamData() {

        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "stream-data-flow");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.serdeFrom(String.class).getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.serdeFrom(String.class).getClass().getName());
        props.setProperty(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG, "40000");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("stream-order-data-flow-input");
        stream.foreach((k, v) -> System.err.println("key " + k + "  value " + v));
        KStream<String, String> filteredStream = stream.filter((k, v) -> v.contains("token"))
                // .mapValues(value -> value.toUpperCase());
                //  .map((k, v) -> new KeyValue<>(k, v.toUpperCase()));
                .map((k, v) -> KeyValue.pair(k, v.toUpperCase()));
        // filtered data would be streamed on output topic
        filteredStream.to("stream-order-data-flow-output");
        //  stream.to("stream-order-data-flow-output");


        Topology topology = builder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        //   System.err.println(topology.describe());
        kafkaStreams.start();


        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }
}
