package com.herojoon.kafkaproject.errorhandler.producer;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Kafka Producer 테스트를 위한 Controller
 */
@RequestMapping("kafka/error")
@RestController
@RequiredArgsConstructor
public class ErrorTestProducer {
    // Spring application.yaml에 정의한 kafka 설정이 주입된 kafkaTemplate
    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * Kafka Producer (Kafka ErrorHandler 테스트)
     * @return
     */
    @GetMapping("producer")
    public String sendMessageForErrorHandler() {
        String errorTopicName = "dev-topic2";
        String messageData = "kafka error test message";
        kafkaTemplate.send(errorTopicName, messageData);
        return "success.";
    }

    /**
     * Kafka Producer (Kafka ErrorHandler 테스트2)
     * @return
     */
    @GetMapping("producer2")
    public String sendMessageForErrorHandlerTwo() {
        String errorTopicName = "dev-topic3";
        String messageData = "kafka error test message two";
        kafkaTemplate.send(errorTopicName, messageData);
        return "success.";
    }
}
