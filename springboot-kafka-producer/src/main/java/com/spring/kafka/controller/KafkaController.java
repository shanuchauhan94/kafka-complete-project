package com.spring.kafka.controller;

import com.spring.kafka.producer.KafkaMessageProduce;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

    private final KafkaMessageProduce messageProduce;

    @Autowired
    public KafkaController(KafkaMessageProduce messageProduce) {
        this.messageProduce = messageProduce;
    }

    @GetMapping(value = "/publish/{product}/{value}")
    public ResponseEntity<Map<String, Object>> publishMessage(@PathVariable("product") String product,
                                                              @PathVariable("value") String value) {

        return new ResponseEntity<>(messageProduce.sendMessage(product,value), HttpStatus.OK);
    }
}
