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
    private static final Schema SCHEMA = new Schema.Parser().parse("""
              {"type":"record","name":"User","namespace":"demo",
               "fields":[{"name":"id","type":"string"},{"name":"name","type":"string"}]}
            """);

    private final KafkaTemplate<String, Object> kafka;

    public ProducerApplication(KafkaTemplate<String, Object> kafka) {
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
        GenericRecord rec = new GenericData.Record(SCHEMA);
        rec.put("id", UUID.randomUUID() + " producer service");
        rec.put("name", UUID.randomUUID() + " producer service");
        kafka.send("practical-work-7.topic", rec);
    }
}
