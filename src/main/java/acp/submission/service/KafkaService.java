package acp.submission.service;

import acp.submission.config.KafkaConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

@Service
public class KafkaService {

    private final KafkaConfig kafkaConfig;
    private final ObjectMapper mapper = new ObjectMapper();

    public KafkaService(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public void writeMessages(String topic, int count, String uid) throws Exception {
        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(kafkaConfig.producerProps())) {
            for (int i = 0; i < count; i++) {
                final int counter = i;
                String msg = mapper.writeValueAsString(
                        new LinkedHashMap<>() {{
                            put("uid", uid);
                            put("counter", counter);
                        }}
                );
                producer.send(new ProducerRecord<>(topic, msg)).get();
            }
            producer.flush();
        }
    }

    public void writeRawMessage(String topic, String message) throws Exception {
        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(kafkaConfig.producerProps())) {
            producer.send(new ProducerRecord<>(topic, message)).get();
            producer.flush();
        }
    }

    public List<String> readMessages(String topic, long timeoutMs) {
        List<String> messages = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(kafkaConfig.consumerProps())) {
            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + timeoutMs - 50;
            while (System.currentTimeMillis() < deadline) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) break;
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(Math.min(remaining, 100)));
                for (ConsumerRecord<String, String> record : records) {
                    messages.add(record.value());
                }
            }
        }
        return messages;
    }

    public List<String> readExactMessages(String topic, int count) {
        List<String> messages = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(kafkaConfig.consumerProps())) {
            consumer.subscribe(Collections.singletonList(topic));
            while (messages.size() < count) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    messages.add(record.value());
                    if (messages.size() >= count) break;
                }
            }
        }
        return messages;
    }
}