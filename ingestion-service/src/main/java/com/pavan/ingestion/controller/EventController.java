package com.pavan.ingestion.controller;

import com.pavan.ingestion.dto.EventRequest;
import com.pavan.ingestion.dto.EventResponse;
import com.pavan.ingestion.service.EventIngestionService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/events")
public class EventController {

    private final EventIngestionService eventIngestionService;

    public EventController(EventIngestionService eventIngestionService) {
        this.eventIngestionService = eventIngestionService;
    }

    @PostMapping
    public ResponseEntity<EventResponse> ingestEvent(
            @Valid @RequestBody EventRequest request) {

        EventResponse response = eventIngestionService.ingest(request);

        return ResponseEntity.accepted().body(response);
    }
}