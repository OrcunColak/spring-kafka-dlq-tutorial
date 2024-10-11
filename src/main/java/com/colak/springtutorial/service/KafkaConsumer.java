package com.colak.springtutorial.service;

import com.colak.springtutorial.model.Payment;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

@Component

@Getter
@Slf4j
public class KafkaConsumer {

    public static final String TOPIC_NAME = "payment-topic";

    // dltTopicSuffix : Specifies the suffix for the Dead Letter Topic (DLT) where failed messages are redirected.
    // dltStrategy : FAIL_ON_ERROR, indicating that the DLQ processing fails if an error occurs.

    // retryTopicSuffix : Sets the suffix for the retry topic, where messages will be retried after encountering errors.
    // attempts : The number of attempts made before the message is sent to the DLQ.
    // If it was 4, the message would be sent to DLQ at 4th attempt
    @RetryableTopic(
            // 1 for testing
            attempts = "1",
            backoff = @Backoff(delay = 3000, multiplier = 10.0)
    )
    @KafkaListener(topics = TOPIC_NAME, groupId = "payment-group")
    public void receive(Payment payment,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Event on topic={}, payload={}", topic, payment);

        if (payment.getId() == 10) {
            // the message will be sent to DQL topic "payment-topic-dlt"
            throw new RuntimeException("my exception");
        }
    }

    @DltHandler
    public void handleDltPayment(Payment payment,
                                 @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic,
                                 @Header(KafkaHeaders.ORIGINAL_TOPIC) String originalTopic,
                                 @Header(KafkaHeaders.EXCEPTION_FQCN) String exceptionFqcn,
                                 @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage,
                                 @Header(KafkaHeaders.EXCEPTION_STACKTRACE) String exceptionStacktrace) {
        // We can Re-queue DLQ Messages
        // See https://medium.com/nerd-for-tech/how-to-re-queue-apache-kafka-dlq-messages-95941525ca77

        // KafkaHeaders.RECEIVED_TOPIC =payment-topic-dlt
        // KafkaHeaders.ORIGINAL_TOPIC =payment-topic
        // KafkaHeaders.EXCEPTION_FQCN =org.springframework.kafka.listener.ListenerExecutionFailedException
        // KafkaHeaders.EXCEPTION_MESSAGE =Listener failed; my exception
        // KafkaHeaders.EXCEPTION_STACKTRACE =org.springframework.kafka.listener.ListenerExecutionFailedException: Listener failed

        log.info("KafkaHeaders.RECEIVED_TOPIC ={}", receivedTopic);
        log.info("KafkaHeaders.ORIGINAL_TOPIC ={}", originalTopic);
        log.info("KafkaHeaders.EXCEPTION_FQCN ={}", exceptionFqcn);
        log.info("KafkaHeaders.EXCEPTION_MESSAGE ={}", exceptionMessage);
        log.info("KafkaHeaders.EXCEPTION_STACKTRACE ={}", exceptionStacktrace);
        log.info("Payload={}", payment);
    }
}
