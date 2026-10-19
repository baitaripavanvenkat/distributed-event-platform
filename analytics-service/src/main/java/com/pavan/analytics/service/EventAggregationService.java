package com.pavan.analytics.service;

import com.pavan.common.event.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;

@Service
public class EventAggregationService {

    private static final Logger log =
            LoggerFactory.getLogger(EventAggregationService.class);

    private static final String METRIC_TYPE = "HOURLY_EVENT_COUNT";

    private final JdbcTemplate jdbcTemplate;

    public EventAggregationService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public void aggregate(EventMessage event) {
        int inserted = jdbcTemplate.update("""
                INSERT INTO analytics_processed_events (event_id)
                VALUES (?)
                ON CONFLICT (event_id) DO NOTHING
                """, event.eventId());

        if (inserted == 0) {
            log.info("Ignoring duplicate analytics event eventId={}", event.eventId());
            return;
        }

        Timestamp eventTimestamp = Timestamp.from(event.timestamp());

        jdbcTemplate.update("""
                INSERT INTO hourly_event_metrics (
                    metric_type,
                    metric_key,
                    window_start,
                    window_end,
                    metric_value,
                    updated_at
                )
                VALUES (
                    ?,
                    ?,
                    date_trunc('hour', CAST(? AS timestamptz)),
                    date_trunc('hour', CAST(? AS timestamptz)) + INTERVAL '1 hour',
                    1,
                    CURRENT_TIMESTAMP
                )
                ON CONFLICT (metric_type, metric_key, window_start)
                DO UPDATE SET
                    metric_value = hourly_event_metrics.metric_value + 1,
                    window_end = EXCLUDED.window_end,
                    updated_at = CURRENT_TIMESTAMP
                """,
                METRIC_TYPE,
                event.eventType(),
                eventTimestamp,
                eventTimestamp
        );

        log.debug("Updated hourly aggregate for eventId={}, eventType={}",
                event.eventId(), event.eventType());
    }
}
