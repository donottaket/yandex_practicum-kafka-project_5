package kz.yandex_practicum.kafka.practical_work_7.consumer;

import org.apache.avro.generic.GenericRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
public class ConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }

    @KafkaListener(topics = "practical-work-7-.topic", groupId = "practical-work-7-group")
    public void onMessage(GenericRecord value) {
        System.out.println("Consume message : " + value);
    }
}