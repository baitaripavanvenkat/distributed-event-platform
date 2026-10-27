package com.pavan.processing.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pavan.common.event.EventMessage;
import com.pavan.processing.repository.EventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class EventProcessingService {

    private static final Logger log =
            LoggerFactory.getLogger(EventProcessingService.class);

    private final EventRepository eventRepository;
    private final ObjectMapper objectMapper;

    public EventProcessingService(
            EventRepository eventRepository,
            ObjectMapper objectMapper) {
        this.eventRepository = eventRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void process(EventMessage event) {

        if (event == null || event.eventId() == null || event.eventId().isBlank()) {
            throw new IllegalArgumentException(
                    "Event and eventId must not be null or blank"
            );
        }

        log.debug("Processing event: eventId={}, eventType={}, userId={}",
                event.eventId(), event.eventType(), event.userId());

        final String payload;

        try {
            payload = objectMapper.writeValueAsString(event.metadata());
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize metadata for eventId={}: {}", event.eventId(), e.getMessage());
            throw new IllegalArgumentException(
                    "Failed to serialize event metadata",
                    e
            );
        }

        int inserted = eventRepository.insertIfAbsent(
                event.eventId(),
                event.eventType(),
                event.userId(),
                event.timestamp(),
                event.source(),
                payload,
                Instant.now()
        );

        if (inserted != 1) {
            log.debug("Duplicate event ignored: eventId={}", event.eventId());
        }
    }
}
