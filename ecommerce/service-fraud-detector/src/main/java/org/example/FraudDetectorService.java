package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.UUID;

public class FraudDetectorService {
    public static void main (String[] args) {
        var fraudService = new FraudDetectorService();

        try(var service = new KafkaService<>(
                FraudDetectorService.class.getSimpleName() + "_" + UUID.randomUUID().toString(),
                "ECOMMERCE_NEW_ORDER",
                fraudService::parse,
                Order.class,
                new HashMap<>())) {

            service.run();
        }
    }

    void parse(ConsumerRecord<String, Order> record) {
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }

        System.out.println("Order processed");
    }
}
