package com.pavan.processing.config;

import com.pavan.common.event.EventMessage;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.messaging.converter.MessageConversionException;

import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:event-processing-group}")
    private String groupId;

    @Value("${spring.kafka.consumer.auto-offset-reset:earliest}")
    private String autoOffsetReset;

    @Value("${spring.kafka.dlt.topic:events.DLT}")
    private String dltTopic;

    @Value("${spring.kafka.listener.concurrency:4}")
    private int concurrency;

    /*
     * Consumer configuration
     */
    @Bean
    public ConsumerFactory<String, EventMessage> consumerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                groupId
        );

        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                autoOffsetReset
        );

        props.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false
        );

        props.put(
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                CooperativeStickyAssignor.class.getName()
        );

        props.put(
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                500
        );

        JacksonJsonDeserializer<EventMessage> jsonDeserializer =
                new JacksonJsonDeserializer<>(EventMessage.class);

        jsonDeserializer.addTrustedPackages(
                "com.pavan.common.event"
        );
        jsonDeserializer.setUseTypeHeaders(false);

        ErrorHandlingDeserializer<String> errorHandlingKeyDeserializer =
                new ErrorHandlingDeserializer<>(new StringDeserializer());
        ErrorHandlingDeserializer<EventMessage> errorHandlingValueDeserializer =
                new ErrorHandlingDeserializer<>(jsonDeserializer);

        return new DefaultKafkaConsumerFactory<>(
                props,
                errorHandlingKeyDeserializer,
                errorHandlingValueDeserializer
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
                bootstrapServers
        );

        props.put(
                ProducerConfig.ACKS_CONFIG,
                "all"
        );

        props.put(
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                true
        );

        props.put(
                ProducerConfig.RETRIES_CONFIG,
                3
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
                                        dltTopic,
                                        record.partition()
                                )
                );

        /*
         * Treat a failed DLT publish as a recovery failure so the offset
         * is not acknowledged if the message could not be sent to the DLT.
         */
        recoverer.setFailIfSendResultIsError(true);

        return recoverer;
    }

    /*
     * Retry transient processing failures three times.
     * Initial attempt + three retries = four attempts.
     * Non-retryable failures bypass retries and are sent directly to the DLT.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            DeadLetterPublishingRecoverer deadLetterPublishingRecoverer
    ) {

        FixedBackOff backOff = new FixedBackOff(
                1000L,
                3L
        );

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                deadLetterPublishingRecoverer,
                backOff
        );

        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                DeserializationException.class,
                ClassCastException.class,
                MessageConversionException.class,
                ConversionException.class
        );

        return errorHandler;
    }

    /*
     * Listener container configuration:
     * AckMode.BATCH ensures offsets are committed after all records in the
     * polled batch are processed, reducing coordinator commit traffic from
     * 1000/sec to ~10-20/sec without acknowledging uncommitted DB transactions.
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
        factory.setConcurrency(concurrency);
        factory.getContainerProperties()
                .setAckMode(ContainerProperties.AckMode.BATCH);

        return factory;
    }

    /*
     * Create the DLT topic if it does not already exist.
     */
    @Bean
    public NewTopic eventsDltTopic() {

        return TopicBuilder.name(dltTopic)
                .partitions(4)
                .replicas(1)
                .build();
    }
}
