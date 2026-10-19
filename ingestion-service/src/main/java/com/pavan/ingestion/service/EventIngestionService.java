package com.pavan.ingestion.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pavan.common.event.EventMessage;
import com.pavan.ingestion.dto.EventRequest;
import com.pavan.ingestion.dto.EventResponse;
import com.pavan.ingestion.exception.EventIdConflictException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class EventIngestionService {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public EventIngestionService(
            JdbcTemplate jdbcTemplate,
            ObjectMapper outboxObjectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = outboxObjectMapper;
    }

    @Transactional
    public EventResponse ingest(EventRequest request) {
        EventMessage event = new EventMessage(
                request.eventId(),
                request.eventType(),
                request.userId(),
                request.timestamp(),
                request.source(),
                request.metadata()
        );

        final String payload;
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize event for the outbox", e);
        }

        int inserted = jdbcTemplate.update("""
                INSERT INTO ingestion_outbox (event_id, payload, status)
                VALUES (?, ?, 'PENDING')
                ON CONFLICT (event_id) DO NOTHING
                """,
                event.eventId(),
                payload
        );

        if (inserted == 0) {
            String existingPayload = jdbcTemplate.queryForObject(
                    "SELECT payload FROM ingestion_outbox WHERE event_id = ?",
                    String.class,
                    event.eventId()
            );

            try {
                EventMessage existingEvent =
                        objectMapper.readValue(existingPayload, EventMessage.class);

                if (!existingEvent.equals(event)) {
                    throw new EventIdConflictException(
                            "eventId '" + event.eventId()
                                    + "' was already used with a different payload"
                    );
                }
            } catch (JsonProcessingException e) {
                throw new IllegalStateException(
                        "Could not read the existing outbox event " + event.eventId(), e);
            }

            return new EventResponse(event.eventId(), "DUPLICATE", Instant.now());
        }

        // ACCEPTED means durably recorded in PostgreSQL. Kafka publication is asynchronous.
        return new EventResponse(event.eventId(), "ACCEPTED", Instant.now());
    }
}
