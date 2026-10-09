package com.pavan.ingestion.service;

import com.pavan.ingestion.dto.EventMessage;
import com.pavan.ingestion.dto.EventRequest;
import com.pavan.ingestion.dto.EventResponse;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class EventIngestionService {

    private final EventProducer eventProducer;

    public EventIngestionService(EventProducer eventProducer) {
        this.eventProducer = eventProducer;
    }

    public EventResponse ingest(EventRequest request) {

        String eventId = "evt-" + UUID.randomUUID();

        EventMessage event = new EventMessage(
                eventId,
                request.eventType(),
                request.userId(),
                request.timestamp(),
                request.source(),
                request.metadata()
        );

        eventProducer.publish(event);

        return new EventResponse(
                eventId,
                "ACCEPTED",
                Instant.now()
        );
    }
}