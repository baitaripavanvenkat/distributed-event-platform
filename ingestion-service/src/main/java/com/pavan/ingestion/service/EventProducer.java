package com.pavan.ingestion.service;

import com.pavan.common.event.EventMessage;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class EventProducer {

    private static final String TOPIC = "events";
    private final KafkaTemplate<String, EventMessage> kafkaTemplate;

    public EventProducer(KafkaTemplate<String, EventMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishAndWait(EventMessage event) throws Exception {
        // Wait for the broker acknowledgement before marking the outbox row as published.
        kafkaTemplate.send(TOPIC, event.eventId(), event)
                .get(10, TimeUnit.SECONDS);
    }
}
