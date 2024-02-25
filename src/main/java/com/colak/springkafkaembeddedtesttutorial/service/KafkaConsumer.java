package com.colak.springkafkaembeddedtesttutorial.service;

import com.colak.springkafkaembeddedtesttutorial.model.Payment;
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
                                 @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Event on dlt topic={}, payload={}", topic, payment);
    }
}
