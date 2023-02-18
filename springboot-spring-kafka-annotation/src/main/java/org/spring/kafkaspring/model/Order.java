package org.spring.kafkaspring.model;


import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
@ToString
public class Order {

    private String id;
    private String product;
    private int quantity;
    private String store;

}
