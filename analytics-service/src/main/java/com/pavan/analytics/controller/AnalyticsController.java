package com.pavan.analytics.controller;

import com.pavan.analytics.dto.HourlyMetricResponse;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/api/v1/analytics")
public class AnalyticsController {

    private final JdbcTemplate jdbcTemplate;

    public AnalyticsController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping("/events/hourly")
    public List<HourlyMetricResponse> getHourlyEventCounts(
            @RequestParam String eventType) {

        return jdbcTemplate.query("""
                SELECT metric_key, window_start, window_end, metric_value
                FROM hourly_event_metrics
                WHERE metric_type = 'HOURLY_EVENT_COUNT'
                  AND metric_key = ?
                  AND window_start >= date_trunc('hour', CURRENT_TIMESTAMP) - INTERVAL '23 hours'
                ORDER BY window_start
                """,
                (rs, rowNum) -> {
                    Timestamp start = rs.getTimestamp("window_start");
                    Timestamp end = rs.getTimestamp("window_end");
                    Instant startInstant = start.toInstant();
                    Instant endInstant = end.toInstant();

                    return new HourlyMetricResponse(
                            rs.getString("metric_key"),
                            startInstant,
                            endInstant,
                            rs.getLong("metric_value")
                    );
                },
                eventType
        );
    }
}
