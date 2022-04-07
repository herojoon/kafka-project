package com.herojoon.kafkaproject.sendmessage.producer;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Kafka Producer 테스트를 위한 Controller
 */
@RequestMapping("kafka/message")
@RestController
@RequiredArgsConstructor
public class MessageProducer {
    // Spring application.yaml에 정의한 kafka 설정이 주입된 kafkaTemplate
    private final KafkaTemplate<String, String> kafkaTemplate;

    private static String TOPIC_NAME = "dev-topic";

    /**
     * Kafka Producer
     * Kafka로 메시지를 전달하는 역할
     * @return
     */
    @GetMapping("producer")
    public String sendMessage() {
        String messageData = "kafka message";
        kafkaTemplate.send(TOPIC_NAME, messageData);
        return "success.";
    }
}
