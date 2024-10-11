package com.colak.springtutorial.config;

import com.colak.springtutorial.model.Payment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    protected Map<String, Object> consumerConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);


        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        return config;
    }

    @Bean
    public ConsumerFactory<String, Payment> consumerFactory() {
        JsonDeserializer<Payment> valueDeserializer = new JsonDeserializer<>();
        valueDeserializer.addTrustedPackages("*");

        Deserializer<String> stringDeserializer = new StringDeserializer();
        return new DefaultKafkaConsumerFactory<>(consumerConfig(), stringDeserializer, valueDeserializer);
    }

    @Bean
    public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory() {
        // factory that creates ConcurrentMessageListenerContainer
        ConcurrentKafkaListenerContainerFactory<String, Payment> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        return factory;
    }
}
