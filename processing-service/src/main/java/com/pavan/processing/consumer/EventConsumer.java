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
            topics = "events",
            groupId = "event-processing-group"
    )
    public void consume(EventMessage event) {
        eventProcessingService.process(event);
    }
}