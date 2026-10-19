package com.pavan.processing.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pavan.common.event.EventMessage;
import com.pavan.processing.repository.EventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class EventProcessingService {

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

        final String payload;

        try {
            payload = objectMapper.writeValueAsString(event.metadata());
        } catch (JsonProcessingException e) {
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
            System.out.println("Duplicate event ignored: " + event.eventId());
        }
    }
}