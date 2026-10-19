package com.pavan.analytics.dto;

import java.time.Instant;

public record HourlyMetricResponse(
        String eventType,
        Instant windowStart,
        Instant windowEnd,
        long totalEvents
) {
}
