package com.pavan.ingestion.dto;

import java.time.Instant;

public record EventResponse(

        String eventId,

        String status,

        Instant receivedAt
) {
}