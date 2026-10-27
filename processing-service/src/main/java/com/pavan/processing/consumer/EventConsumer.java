package com.pavan.processing.consumer;

import com.pavan.common.event.EventMessage;
import com.pavan.processing.service.EventProcessingService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class EventConsumer {

    private final EventProcessingService eventProcessingService;

    public EventConsumer(
            EventProcessingService eventProcessingService) {
        this.eventProcessingService = eventProcessingService;
    }

    @KafkaListener(
            topics = "${spring.kafka.consumer.topic:events}",
            groupId = "${spring.kafka.consumer.group-id:event-processing-group}",
            concurrency = "${spring.kafka.listener.concurrency:4}"
    )
    public void consume(EventMessage event) {
        eventProcessingService.process(event);
    }
}