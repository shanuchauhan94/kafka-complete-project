package org.spring.kafka.stream.producer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCountProducer {

    public static void streamData() {

        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-stream-flow");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.serdeFrom(String.class).getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.serdeFrom(String.class).getClass().getName());
        props.setProperty(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG, "40000");
        // used by kTable to persist the records
        props.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0"); // default 1

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("word-count-stream-input");
        stream.flatMapValues(value -> Arrays.asList(value.toLowerCase().split(" ")))
                // works on keys only .  make value as key
                .groupBy((k, v) -> v)
                .count()
                .toStream()
                .to("word-count-stream-output", Produced.with(Serdes.serdeFrom(String.class), Serdes.serdeFrom(Long.class)));

        Topology topology = builder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        //   System.err.println(topology.describe());
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }
}
