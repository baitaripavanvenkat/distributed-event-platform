package com.pavan.analytics.config;

import com.pavan.common.event.EventMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, EventMessage> consumerFactory(
            org.springframework.core.env.Environment environment) {

        Map<String, Object> props = new HashMap<>();

        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                environment.getProperty(
                        "spring.kafka.bootstrap-servers",
                        "localhost:9092"
                )
        );

        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                environment.getProperty(
                        "spring.kafka.consumer.group-id",
                        "event-analytics-group"
                )
        );

        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                environment.getProperty(
                        "spring.kafka.consumer.auto-offset-reset",
                        "earliest"
                )
        );

        props.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false
        );

        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                JacksonJsonDeserializer.class
        );

        props.put(
                "spring.json.value.default.type",
                EventMessage.class.getName()
        );

        props.put(
                "spring.json.trusted.packages",
                "com.pavan.common.event"
        );

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, EventMessage>
    kafkaListenerContainerFactory(
            ConsumerFactory<String, EventMessage> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, EventMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        return factory;
    }
}
