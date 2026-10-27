package com.pavan.ingestion.service;

import com.pavan.common.event.EventMessage;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class EventProducer {

    private static final String TOPIC = "events";

    private final KafkaTemplate<String, EventMessage> kafkaTemplate;

    public EventProducer(KafkaTemplate<String, EventMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Starts publishing without blocking the calling thread. The caller must
     * wait for the returned future before marking the outbox row as published.
     */
    public CompletableFuture<SendResult<String, EventMessage>> publishAsync(EventMessage event) {
        return kafkaTemplate.send(TOPIC, event.eventId(), event);
    }
}
