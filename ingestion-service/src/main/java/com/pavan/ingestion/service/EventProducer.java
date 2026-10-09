package com.pavan.ingestion.service;
import com.pavan.ingestion.dto.EventMessage;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class EventProducer {
    private static final String TOPIC = "events";

    private final KafkaTemplate<String, EventMessage> kafkaTemplate;

    public EventProducer(
            KafkaTemplate<String, EventMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(EventMessage event) {

        kafkaTemplate.send(
                TOPIC,
                event.eventId(),
                event
        );
    }
}
