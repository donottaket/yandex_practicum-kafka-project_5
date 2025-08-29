package kz.yandex_practicum.kafka.practical_work_7.producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;

@SpringBootApplication
public class ProducerApplication {
    private final KafkaTemplate<String, String> kafka;

    public ProducerApplication(KafkaTemplate<String, String> kafka) {
        this.kafka = kafka;
    }

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() throws InterruptedException {
        while (true) {
            Thread.sleep(2_000);
            send();
        }
    }

    public void send() {
        kafka.send("practical-work-7-input.topic", UUID.randomUUID().toString());
    }
}
