package com.pavan.processing.dto;

import java.time.Instant;
import java.util.Map;

public record EventMessage(

        String eventId,

        String eventType,

        String userId,

        Instant timestamp,

        String source,

        Map<String, Object> metadata
) {
}