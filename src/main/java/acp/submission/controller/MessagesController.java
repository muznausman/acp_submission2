package acp.submission.controller;

import acp.submission.dto.SplitterRequest;
import acp.submission.dto.TransformRequest;
import acp.submission.service.KafkaService;
import acp.submission.service.RabbitMQService;
import acp.submission.service.RedisService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

@RestController
@RequestMapping("/api/v1/acp")
public class MessagesController {

    private static final String UID = "s2883214";

    private final RabbitMQService rabbitMQService;
    private final KafkaService kafkaService;
    private final RedisService redisService;
    private final ObjectMapper mapper = new ObjectMapper();

    public MessagesController(RabbitMQService rabbitMQService,
                              KafkaService kafkaService,
                              RedisService redisService) {
        this.rabbitMQService = rabbitMQService;
        this.kafkaService = kafkaService;
        this.redisService = redisService;
    }

    // (2) PUT messages/rabbitmq/{queueName}/{messageCount}
    @PutMapping("/messages/rabbitmq/{queueName}/{messageCount}")
    public ResponseEntity<?> writeRabbitMQ(@PathVariable String queueName,
                                           @PathVariable int messageCount) {
        try {
            rabbitMQService.writeMessages(queueName, messageCount, UID);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.status(500).body(e.getMessage());
        }
    }

    // (2) PUT messages/kafka/{writeTopic}/{messageCount}
    @PutMapping("/messages/kafka/{writeTopic}/{messageCount}")
    public ResponseEntity<?> writeKafka(@PathVariable String writeTopic,
                                        @PathVariable int messageCount) {
        try {
            kafkaService.writeMessages(writeTopic, messageCount, UID);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.status(500).body(e.getMessage());
        }
    }

    // (2) GET messages/rabbitmq/{queueName}/{timeoutInMsec}
    @GetMapping("/messages/rabbitmq/{queueName}/{timeoutInMsec}")
    public ResponseEntity<?> readRabbitMQ(@PathVariable String queueName,
                                          @PathVariable long timeoutInMsec) {
        try {
            List<String> messages = rabbitMQService.readMessages(queueName, timeoutInMsec);
            return ResponseEntity.ok(messages);
        } catch (Exception e) {
            return ResponseEntity.status(500).body(e.getMessage());
        }
    }

    // (2) GET messages/kafka/{readTopic}/{timeoutInMsec}
    @GetMapping("/messages/kafka/{readTopic}/{timeoutInMsec}")
    public ResponseEntity<?> readKafka(@PathVariable String readTopic,
                                       @PathVariable long timeoutInMsec) {
        try {
            List<String> messages = kafkaService.readMessages(readTopic, timeoutInMsec);
            return ResponseEntity.ok(messages);
        } catch (Exception e) {
            return ResponseEntity.status(500).body(e.getMessage());
        }
    }

    // (3) GET messages/sorted/rabbitmq/{queueName}/{messagesToConsider}
    @GetMapping("/messages/sorted/rabbitmq/{queueName}/{messagesToConsider}")
    public ResponseEntity<?> readSortedRabbitMQ(@PathVariable String queueName,
                                                @PathVariable int messagesToConsider) {
        try {
            List<String> raw = rabbitMQService.readExactMessages(queueName, messagesToConsider);
            List<String> sorted = sortById(raw);
            return ResponseEntity.ok(sorted);
        } catch (Exception e) {
            return ResponseEntity.status(500).body(e.getMessage());
        }
    }

    // (3) GET messages/sorted/kafka/{topic}/{messagesToConsider}
    @GetMapping("/messages/sorted/kafka/{topic}/{messagesToConsider}")
    public ResponseEntity<?> readSortedKafka(@PathVariable String topic,
                                             @PathVariable int messagesToConsider) {
        try {
            List<String> raw = kafkaService.readExactMessages(topic, messagesToConsider);
            List<String> sorted = sortById(raw);
            return ResponseEntity.ok(sorted);
        } catch (Exception e) {
            return ResponseEntity.status(500).body(e.getMessage());
        }
    }

    // (7) POST splitter
    @PostMapping("/splitter")
    public ResponseEntity<?> splitter(@RequestBody SplitterRequest req) {
        try {
            List<String> messages = rabbitMQService.readExactMessages(
                    req.getReadQueue(), req.getMessageCount());

            // Load existing redis counters
            long countEven = parseLong(redisService.get("count_even"));
            long countOdd = parseLong(redisService.get("count_odd"));
            double sumEven = parseDouble(redisService.get("average_even")) * countEven;
            double sumOdd = parseDouble(redisService.get("average_odd")) * countOdd;

            for (String raw : messages) {
                JsonNode node = mapper.readTree(raw);
                int id = node.path("Id").asInt();
                double value = node.path("Value").asDouble();
                String json = mapper.writeValueAsString(node);

                if (id % 2 == 0) {
                    redisService.hset(req.getRedisHashEven(), String.valueOf(id), json);
                    try {
                        kafkaService.writeRawMessage(req.getWriteTopicEven(), raw);
                    } catch (Exception ignored) {}
                    countEven++;
                    sumEven += value;
                } else {
                    redisService.hset(req.getRedisHashOdd(), String.valueOf(id), json);
                    try {
                        kafkaService.writeRawMessage(req.getWriteTopicOdd(), raw);
                    } catch (Exception ignored) {}
                    countOdd++;
                    sumOdd += value;
                }
            }

            // Save updated counters
            redisService.set("count_even", String.valueOf(countEven));
            redisService.set("count_odd", String.valueOf(countOdd));

            double avgEven = countEven > 0 ?
                    BigDecimal.valueOf(sumEven / countEven)
                            .setScale(2, RoundingMode.HALF_UP).doubleValue() : 0.0;
            double avgOdd = countOdd > 0 ?
                    BigDecimal.valueOf(sumOdd / countOdd)
                            .setScale(2, RoundingMode.HALF_UP).doubleValue() : 0.0;

            redisService.set("average_even", String.valueOf(avgEven));
            redisService.set("average_odd", String.valueOf(avgOdd));

            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.status(500).body(e.getMessage());
        }
    }

    // (9) POST transformMessages
    @PostMapping("/transformMessages")
    public ResponseEntity<?> transformMessages(@RequestBody TransformRequest req) {
        try {
            int totalWritten = 0;
            int totalProcessed = 0;
            int totalRedisUpdates = 0;
            double totalValueWritten = 0.0;
            double totalAdded = 0.0;

            for (int i = 0; i < req.getMessageCount(); i++) {
                String raw = rabbitMQService.readExactMessages(req.getReadQueue(), 1).get(0);
                JsonNode node = mapper.readTree(raw);
                String key = node.path("key").asText();

                if ("TOMBSTONE".equals(key)) {
                    redisService.deleteAll();
                    totalRedisUpdates++;

                    Map<String, Object> summary = new LinkedHashMap<>();
                    summary.put("totalMessagesWritten", totalWritten);
                    summary.put("totalMessagesProcessed", totalProcessed);
                    summary.put("totalRedisUpdates", totalRedisUpdates);
                    summary.put("totalValueWritten", totalValueWritten);
                    summary.put("totalAdded", totalAdded);
                    rabbitMQService.writeRawMessage(req.getWriteQueue(),
                            mapper.writeValueAsString(summary));
                } else {
                    int version = node.path("version").asInt();
                    double value = node.path("value").asDouble();

                    String storedVersionStr = redisService.get(key);
                    boolean shouldProcess = false;

                    if (storedVersionStr == null) {
                        shouldProcess = true;
                    } else {
                        int storedVersion = Integer.parseInt(storedVersionStr);
                        if (version > storedVersion) {
                            shouldProcess = true;
                        }
                    }

                    String outJson;
                    if (shouldProcess) {
                        redisService.set(key, String.valueOf(version));
                        double newValue = value + 10.5;
                        ObjectNode outNode = (ObjectNode) node.deepCopy();
                        outNode.put("value", newValue);
                        outJson = mapper.writeValueAsString(outNode);
                        totalProcessed++;
                        totalRedisUpdates++;
                        totalAdded += 10.5;
                        totalValueWritten += newValue;
                    } else {
                        outJson = raw;
                        totalValueWritten += value;
                    }

                    rabbitMQService.writeRawMessage(req.getWriteQueue(), outJson);
                    totalWritten++;
                }
            }

            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.status(500).body(e.getMessage());
        }
    }

    // Helper: sort messages by Id field
    private List<String> sortById(List<String> messages) {
        List<String> sorted = new ArrayList<>(messages);
        sorted.sort((a, b) -> {
            try {
                int idA = mapper.readTree(a).path("Id").asInt();
                int idB = mapper.readTree(b).path("Id").asInt();
                return Integer.compare(idA, idB);
            } catch (Exception e) {
                return 0;
            }
        });
        return sorted;
    }

    private long parseLong(String val) {
        if (val == null || val.isBlank()) return 0L;
        try { return Long.parseLong(val); } catch (Exception e) { return 0L; }
    }

    private double parseDouble(String val) {
        if (val == null || val.isBlank()) return 0.0;
        try { return Double.parseDouble(val); } catch (Exception e) { return 0.0; }
    }
}