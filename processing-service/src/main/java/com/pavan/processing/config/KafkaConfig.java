package com.pavan.processing.config;

import com.pavan.common.event.EventMessage;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.kafka.listener.ContainerProperties;

import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private static final String DLT_TOPIC = "events.DLT";

    /*
     * Consumer configuration
     */
    @Bean
    public ConsumerFactory<String, EventMessage> consumerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS
        );

        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "event-processing-group"
        );

        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        props.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false
        );

        JacksonJsonDeserializer<EventMessage> jsonDeserializer =
                new JacksonJsonDeserializer<>(EventMessage.class);

        jsonDeserializer.addTrustedPackages(
                "com.pavan.common.event"
        );

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                jsonDeserializer
        );
    }

    /*
     * Producer configuration for publishing failed events to the DLT
     */
    @Bean
    public ProducerFactory<String, EventMessage> dltProducerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS
        );

        props.put(
                ProducerConfig.ACKS_CONFIG,
                "all"
        );

        return new DefaultKafkaProducerFactory<>(
                props,
                new StringSerializer(),
                new JacksonJsonSerializer<EventMessage>()
        );
    }

    /*
     * KafkaTemplate used by the DeadLetterPublishingRecoverer
     */
    @Bean
    public KafkaTemplate<String, EventMessage> dltKafkaTemplate(
            ProducerFactory<String, EventMessage> dltProducerFactory
    ) {
        return new KafkaTemplate<>(dltProducerFactory);
    }

    /*
     * Route failed records to events.DLT.
     * Preserve the original partition.
     */
    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaTemplate<String, EventMessage> dltKafkaTemplate
    ) {

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        dltKafkaTemplate,
                        (record, exception) ->
                                new TopicPartition(
                                        DLT_TOPIC,
                                        record.partition()
                                )
                );

        /*
         * Treat a failed DLT publish as a recovery failure.
         */
        recoverer.setFailIfSendResultIsError(true);

        return recoverer;
    }

    /*
     * Retry failed processing three times.
     * Initial attempt + three retries = four attempts.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            DeadLetterPublishingRecoverer deadLetterPublishingRecoverer
    ) {

        FixedBackOff backOff = new FixedBackOff(
                1000L,
                3L
        );

        return new DefaultErrorHandler(
                deadLetterPublishingRecoverer,
                backOff
        );
    }

    /*
     * Listener container configuration
     */
    @Bean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, EventMessage>
    kafkaListenerContainerFactory(
            ConsumerFactory<String, EventMessage> consumerFactory,
            DefaultErrorHandler kafkaErrorHandler
    ) {

        ConcurrentKafkaListenerContainerFactory<String, EventMessage>
                factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        factory.setCommonErrorHandler(kafkaErrorHandler);

        factory.getContainerProperties()
                .setAckMode(ContainerProperties.AckMode.RECORD);

        return factory;
    }

    /*
     * Create the DLT topic if it does not already exist.
     */
    @Bean
    public NewTopic eventsDltTopic() {

        return TopicBuilder.name(DLT_TOPIC)
                .partitions(4)
                .replicas(1)
                .build();
    }
}