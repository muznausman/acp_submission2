package acp.submission.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class RabbitMQService {

    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper mapper = new ObjectMapper();

    public RabbitMQService(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void writeMessages(String queueName, int count, String uid) throws Exception {
        for (int i = 0; i < count; i++) {
            final int counter = i;
            String msg = mapper.writeValueAsString(
                    new java.util.LinkedHashMap<>() {{
                        put("uid", uid);
                        put("counter", counter);
                    }}
            );
            rabbitTemplate.convertAndSend(queueName, msg);
        }
    }

    public void writeRawMessage(String queueName, String message) {
        rabbitTemplate.convertAndSend(queueName, message);
    }

    public List<String> readMessages(String queueName, long timeoutMs) {
        List<String> messages = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutMs - 50;

        while (System.currentTimeMillis() < deadline) {
            try {
                Object msg = rabbitTemplate.receiveAndConvert(queueName, 50);
                if (msg != null) {
                    if (msg instanceof byte[]) {
                        messages.add(new String((byte[]) msg));
                    } else {
                        messages.add(msg.toString());
                    }
                }
            } catch (Exception e) {
                break;
            }
        }
        return messages;
    }

    public List<String> readExactMessages(String queueName, int count) {
        List<String> messages = new ArrayList<>();
        while (messages.size() < count) {
            try {
                Object msg = rabbitTemplate.receiveAndConvert(queueName, 1000);
                if (msg != null) {
                    if (msg instanceof byte[]) {
                        messages.add(new String((byte[]) msg));
                    } else {
                        messages.add(msg.toString());
                    }
                }
            } catch (Exception e) {
                // continue waiting
            }
        }
        return messages;
    }
}