package com.pavan.processing.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pavan.common.event.EventMessage;
import com.pavan.processing.repository.EventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventProcessingServiceTest {

    @Mock
    private EventRepository eventRepository;

    @Mock
    private ObjectMapper objectMapper;

    private EventProcessingService eventProcessingService;

    @BeforeEach
    void setUp() {
        eventProcessingService = new EventProcessingService(eventRepository, objectMapper);
    }

    private EventMessage createSampleEvent(String eventId) {
        return new EventMessage(
                eventId,
                "ORDER_PLACED",
                "user-100",
                Instant.now(),
                "web-client",
                Map.of("amount", 250.0, "currency", "USD")
        );
    }

    @Test
    @DisplayName("Valid event is successfully serialized and persisted via insertIfAbsent")
    void testProcess_ValidEvent_PersistsSuccessfully() throws Exception {
        EventMessage event = createSampleEvent("event-001");
        String payloadJson = "{\"amount\":250.0,\"currency\":\"USD\"}";

        when(objectMapper.writeValueAsString(event.metadata())).thenReturn(payloadJson);
        when(eventRepository.insertIfAbsent(
                eq(event.eventId()),
                eq(event.eventType()),
                eq(event.userId()),
                eq(event.timestamp()),
                eq(event.source()),
                eq(payloadJson),
                any(Instant.class)
        )).thenReturn(1);

        eventProcessingService.process(event);

        verify(objectMapper).writeValueAsString(event.metadata());
        verify(eventRepository).insertIfAbsent(
                eq("event-001"),
                eq("ORDER_PLACED"),
                eq("user-100"),
                eq(event.timestamp()),
                eq("web-client"),
                eq(payloadJson),
                any(Instant.class)
        );
    }

    @Test
    @DisplayName("Duplicate event returns 0 from insertIfAbsent and is handled idempotently without error")
    void testProcess_DuplicateEvent_HandledIdempotently() throws Exception {
        EventMessage event = createSampleEvent("event-dup-001");
        String payloadJson = "{\"amount\":250.0,\"currency\":\"USD\"}";

        when(objectMapper.writeValueAsString(event.metadata())).thenReturn(payloadJson);
        when(eventRepository.insertIfAbsent(
                eq(event.eventId()),
                eq(event.eventType()),
                eq(event.userId()),
                eq(event.timestamp()),
                eq(event.source()),
                eq(payloadJson),
                any(Instant.class)
        )).thenReturn(0);

        // Must complete without throwing an exception
        eventProcessingService.process(event);

        verify(eventRepository).insertIfAbsent(
                eq("event-dup-001"),
                eq("ORDER_PLACED"),
                eq("user-100"),
                eq(event.timestamp()),
                eq("web-client"),
                eq(payloadJson),
                any(Instant.class)
        );
    }

    @Test
    @DisplayName("Metadata serialization failure throws non-retryable IllegalArgumentException")
    void testProcess_SerializationFailure_ThrowsIllegalArgumentException() throws Exception {
        EventMessage event = createSampleEvent("event-bad-meta");

        when(objectMapper.writeValueAsString(any()))
                .thenThrow(new JsonProcessingException("Serialization failed") {});

        IllegalArgumentException thrown = assertThrows(
                IllegalArgumentException.class,
                () -> eventProcessingService.process(event)
        );

        assertEquals("Failed to serialize event metadata", thrown.getMessage());
        assertNotNull(thrown.getCause());
        verify(eventRepository, never()).insertIfAbsent(any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    @DisplayName("Database repository exception is not swallowed and surfaces to trigger rollback")
    void testProcess_DatabaseFailure_SurfacesException() throws Exception {
        EventMessage event = createSampleEvent("event-db-fail");
        String payloadJson = "{\"amount\":250.0}";

        when(objectMapper.writeValueAsString(event.metadata())).thenReturn(payloadJson);
        when(eventRepository.insertIfAbsent(any(), any(), any(), any(), any(), any(), any()))
                .thenThrow(new DataIntegrityViolationException("Database connection terminated"));

        assertThrows(
                DataIntegrityViolationException.class,
                () -> eventProcessingService.process(event)
        );
    }
}
