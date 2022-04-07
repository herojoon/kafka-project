package com.herojoon.kafkaproject.errorhandler.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ErrorTestListener {

    /**
     * Kafka errorHandler를 이용하여 error처리하기 (방법1)
     *
     * <설명>
     * KafkaListenerErrorHandler를 구현해 놓은 KafkaErrorHandler.class의 이름을
     * @KafkaListener(errorHandler="이곳에 입력해줍니다.")
     * KafkaErrorHandler의 앞자리를 소문자로 입력해줍니다.
     *
     * <KafkaListenerErrorHandler 구현 위치>
     *  com.herojoon.kafkaproject.errorhandler.exception.KafkaErrorHandler
     *
     * @param record
     */
    @KafkaListener(topics = "dev-topic2", errorHandler = "kafkaErrorHandler")
    public void errorTestListener(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        log.info("### record: " + record.toString());
        log.info("### topic: " + record.topic() + ", value: " + record.value() + ", offset: " + record.offset());
        throw new KafkaException("무슨무슨 에러가 발생하였다..!!");  // KafkaListenerErrorHandler를 테스트하기 위해 에러를 발생시킵니다.
    }

    /**
     * Kafka errorHandler를 이용하여 error처리하기 (방법2)
     *
     * <설명>
     * KafkaListenerErrorHandler를 구현해 놓은 KafkaErrorHandlerTwo()의 이름을
     * @KafkaListener(errorHandler="이곳에 입력해줍니다.")
     * KafkaErrorHandlerTwo의 앞자리를 소문자로 입력해줍니다.
     *
     * <KafkaListenerErrorHandler 구현 위치>
     * com.herojoon.kafkaproject.errorhandler.config.kafkaConfig의 KafkaErrorHandlerTwo()
     *
     * @param record
     */
    @KafkaListener(topics = "dev-topic3", errorHandler = "kafkaErrorHandlerTwo")
    @SendTo("dev-dead-topic")
    public void errorTestListenerTow(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        log.info("### record: " + record.toString());
        log.info("### topic: " + record.topic() + ", value: " + record.value() + ", offset: " + record.offset());
        throw new KafkaException("무슨무슨 에러가 발생하였다..!! Two");  // KafkaListenerErrorHandler를 테스트하기 위해 에러를 발생시킵니다.
    }

    /**
     * 실패한 Kafka 메시지를 전달받아 처리하는 Consumer
     *
     * @param record
     * @param acknowledgment
     */
    @KafkaListener(topics = "dev-dead-topic")
    public void deadTopicConsumer(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        log.info("### [dead-topic] record: " + record.toString());
        log.info("### [dead-topic] topic: " + record.topic() + ", value: " + record.value() + ", offset: " + record.offset());

        // kafka 메시지 읽어온 곳까지 commit. (이 부분을 하지 않으면 메시지를 소비했다고 commit 된 것이 아니므로 계속 메시지를 읽어온다)
        acknowledgment.acknowledge();
    }
}
