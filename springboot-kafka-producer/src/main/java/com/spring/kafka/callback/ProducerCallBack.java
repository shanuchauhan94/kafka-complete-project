package com.spring.kafka.callback;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.Map;

public class ProducerCallBack implements Callback {

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {

        Map<String, Object> map = new HashMap<>();
        map.put("Event Type ", "Asynchronous");
        map.put("Topic Name ", recordMetadata.topic());
        map.put("Partition Number ", recordMetadata.partition());
        map.put("Offset Name ", recordMetadata.offset());
        map.put("Message send Asynchronously ", true);
        System.err.println(map);

        if (e != null) {
            e.printStackTrace();
        }


    }
}
