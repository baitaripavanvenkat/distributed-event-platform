package com.pavan.ingestion.dto;


import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.Map;

public record EventRequest(

        @NotBlank(message = "eventType is required")
        String eventType,

        @NotBlank(message = "userId is required")
        String userId,

        @NotNull(message = "timestamp is required")
        Instant timestamp,

        @NotBlank(message = "source is required")
        String source,

        Map<String, Object> metadata
) {
}