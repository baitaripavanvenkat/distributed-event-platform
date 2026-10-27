package com.pavan.processing.config;

import com.pavan.common.event.EventMessage;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExceptionMatcher;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaConfigTest {

    private KafkaConfig kafkaConfig;

    @BeforeEach
    void setUp() {
        kafkaConfig = new KafkaConfig();
        ReflectionTestUtils.setField(kafkaConfig, "bootstrapServers", "localhost:9092");
        ReflectionTestUtils.setField(kafkaConfig, "groupId", "event-processing-group");
        ReflectionTestUtils.setField(kafkaConfig, "autoOffsetReset", "earliest");
        ReflectionTestUtils.setField(kafkaConfig, "dltTopic", "events.DLT");
        ReflectionTestUtils.setField(kafkaConfig, "concurrency", 4);
    }

    @Test
    @DisplayName("Kafka listener container factory AckMode is configured to BATCH and concurrency is 4")
    void testKafkaListenerContainerFactory_AckModeIsBatchAndConcurrencyIs4() {
        ConsumerFactory<String, EventMessage> consumerFactory = kafkaConfig.consumerFactory();
        ProducerFactory<String, EventMessage> producerFactory = kafkaConfig.dltProducerFactory();
        KafkaTemplate<String, EventMessage> kafkaTemplate = kafkaConfig.dltKafkaTemplate(producerFactory);
        DeadLetterPublishingRecoverer recoverer = kafkaConfig.deadLetterPublishingRecoverer(kafkaTemplate);
        DefaultErrorHandler errorHandler = kafkaConfig.kafkaErrorHandler(recoverer);

        ConcurrentKafkaListenerContainerFactory<String, EventMessage> factory =
                kafkaConfig.kafkaListenerContainerFactory(consumerFactory, errorHandler);

        assertEquals(
                ContainerProperties.AckMode.BATCH,
                factory.getContainerProperties().getAckMode(),
                "AckMode must be BATCH to prevent overwhelming coordinator with per-record offset commits"
        );
        assertEquals(
                4,
                ReflectionTestUtils.getField(factory, "concurrency"),
                "Concurrency must match the 4 partitions of the events topic"
        );
    }

    @Test
    @DisplayName("ConsumerFactory wraps key and value deserializers with ErrorHandlingDeserializer and configures CooperativeStickyAssignor")
    void testConsumerFactory_UsesErrorHandlingDeserializerAndCooperativeSticky() {
        ConsumerFactory<String, EventMessage> consumerFactory = kafkaConfig.consumerFactory();
        assertNotNull(consumerFactory);
        assertTrue(consumerFactory instanceof DefaultKafkaConsumerFactory);

        DefaultKafkaConsumerFactory<String, EventMessage> defaultFactory =
                (DefaultKafkaConsumerFactory<String, EventMessage>) consumerFactory;

        assertTrue(
                defaultFactory.getKeyDeserializer() instanceof ErrorHandlingDeserializer,
                "Key deserializer must be wrapped in ErrorHandlingDeserializer"
        );
        assertTrue(
                defaultFactory.getValueDeserializer() instanceof ErrorHandlingDeserializer,
                "Value deserializer must be wrapped in ErrorHandlingDeserializer"
        );

        Map<String, Object> config = defaultFactory.getConfigurationProperties();
        assertEquals("localhost:9092", config.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("event-processing-group", config.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals("earliest", config.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(false, config.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertEquals(
                CooperativeStickyAssignor.class.getName(),
                config.get(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG)
        );
        assertEquals(500, config.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    }

    @Test
    @DisplayName("DLT ProducerFactory has idempotence enabled and ACKS=all")
    void testDltProducerFactory_Configuration() {
        ProducerFactory<String, EventMessage> producerFactory = kafkaConfig.dltProducerFactory();
        assertNotNull(producerFactory);
        assertTrue(producerFactory instanceof DefaultKafkaProducerFactory);

        DefaultKafkaProducerFactory<String, EventMessage> defaultFactory =
                (DefaultKafkaProducerFactory<String, EventMessage>) producerFactory;

        Map<String, Object> config = defaultFactory.getConfigurationProperties();
        assertEquals("localhost:9092", config.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("all", config.get(ProducerConfig.ACKS_CONFIG));
        assertEquals(true, config.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
    }

    @Test
    @DisplayName("DeadLetterPublishingRecoverer is configured with failIfSendResultIsError=true")
    void testDeadLetterPublishingRecoverer_FailsIfSendResultIsError() {
        ProducerFactory<String, EventMessage> producerFactory = kafkaConfig.dltProducerFactory();
        KafkaTemplate<String, EventMessage> kafkaTemplate = kafkaConfig.dltKafkaTemplate(producerFactory);
        DeadLetterPublishingRecoverer recoverer = kafkaConfig.deadLetterPublishingRecoverer(kafkaTemplate);

        assertNotNull(recoverer);
        // Field failIfSendResultIsError must be true so unpersisted DLT messages are not committed
        Object failOnError = ReflectionTestUtils.getField(recoverer, "failIfSendResultIsError");
        assertEquals(Boolean.TRUE, failOnError);
    }

    @Test
    @DisplayName("DefaultErrorHandler marks non-retryable exceptions properly")
    void testDefaultErrorHandler_NonRetryableExceptions() {
        ProducerFactory<String, EventMessage> producerFactory = kafkaConfig.dltProducerFactory();
        KafkaTemplate<String, EventMessage> kafkaTemplate = kafkaConfig.dltKafkaTemplate(producerFactory);
        DeadLetterPublishingRecoverer recoverer = kafkaConfig.deadLetterPublishingRecoverer(kafkaTemplate);
        DefaultErrorHandler errorHandler = kafkaConfig.kafkaErrorHandler(recoverer);

        assertNotNull(errorHandler);
        ExceptionMatcher matcher = (ExceptionMatcher) ReflectionTestUtils.invokeMethod(errorHandler, "getExceptionMatcher");
        assertNotNull(matcher);

        Map<Class<? extends Throwable>, Boolean> entries = (Map<Class<? extends Throwable>, Boolean>) ReflectionTestUtils.getField(matcher, "entries");
        assertNotNull(entries);

        assertEquals(Boolean.FALSE, entries.get(IllegalArgumentException.class), "IllegalArgumentException must be classified as non-retryable (false)");
        assertEquals(Boolean.FALSE, entries.get(DeserializationException.class), "DeserializationException must be classified as non-retryable (false)");
        assertEquals(Boolean.FALSE, entries.get(ClassCastException.class), "ClassCastException must be classified as non-retryable (false)");
        assertEquals(Boolean.FALSE, entries.get(MessageConversionException.class), "MessageConversionException must be classified as non-retryable (false)");
        assertEquals(Boolean.FALSE, entries.get(ConversionException.class), "ConversionException must be classified as non-retryable (false)");
    }

    @Test
    @DisplayName("DLT topic NewTopic bean has 4 partitions and replication factor 1")
    void testEventsDltTopic_Definition() {
        NewTopic dltTopic = kafkaConfig.eventsDltTopic();
        assertNotNull(dltTopic);
        assertEquals("events.DLT", dltTopic.name());
        assertEquals(4, dltTopic.numPartitions());
        assertEquals((short) 1, dltTopic.replicationFactor());
    }
}
