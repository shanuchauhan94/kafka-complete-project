package org.spring.kafkaspring.controller;

import org.spring.kafkaspring.producer.OrderProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka/publish")
public class UserController {

    private final OrderProducerService service;

    @Autowired
    public UserController(OrderProducerService service) {
        this.service = service;
    }

    @GetMapping("/user/{store}")
    public ResponseEntity<String> sendUserData(@PathVariable("store") String store) {
        service.sendUserDate(store);
        return new ResponseEntity<>("data published successfully.", HttpStatus.OK);

    }
}
