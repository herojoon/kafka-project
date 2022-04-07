package com.herojoon.kafkaproject.errorhandler.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;

@Slf4j
@EnableKafka
@Configuration
public class KafkaConfig {

    /**
     * kafka error 발생 시 처리
     *
     * <kafka error 발생 시 처리 방법>
     * 에러 발생 시 로그를 기록하거나 error처리를 위한 kafka topic으로 재전송하는 동작을 할 수 있습니다.
     *
     * @return
     */
    @Bean
    public KafkaListenerErrorHandler kafkaErrorHandlerTwo() {
        return (m, e) -> {
            /**
             * error 로그 기록
             */
            log.error("[KafkaErrorHandler] kafkaMessage=[" + m.getPayload() + "], errorMessage=[" + e.getMessage() + "]");

            ConsumerRecord<String, String> record = (ConsumerRecord<String, String>) m.getPayload();
            // 메시지를 더 가공하거나 별도 처리를 하고..
            
            return record.value();  // sendTo("토픽명")에 입력된 토픽으로 전달 될 메시지 내용 
        };
    }
}
