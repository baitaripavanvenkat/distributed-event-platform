package com.pavan.ingestion.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pavan.common.event.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final JdbcTemplate jdbcTemplate;
    private final EventProducer eventProducer;
    private final ObjectMapper objectMapper;
    private final int batchSize;

    public OutboxPublisher(
            JdbcTemplate jdbcTemplate,
            EventProducer eventProducer,
            ObjectMapper outboxObjectMapper,
            @Value("${outbox.publisher.batch-size:100}") int batchSize) {
        this.jdbcTemplate = jdbcTemplate;
        this.eventProducer = eventProducer;
        this.objectMapper = outboxObjectMapper;
        this.batchSize = Math.max(1, batchSize);
    }

    @Scheduled(fixedDelayString = "${outbox.publisher.fixed-delay-ms:1000}")
    public void publishPendingEvents() {
        List<OutboxRecord> pending = jdbcTemplate.query(
                """
                SELECT event_id, payload
                FROM ingestion_outbox
                WHERE status = 'PENDING'
                ORDER BY created_at
                LIMIT ?
                """,
                (rs, rowNum) -> new OutboxRecord(
                        rs.getString("event_id"),
                        rs.getString("payload")
                ),
                batchSize
        );

        for (OutboxRecord record : pending) {
            try {
                EventMessage event =
                        objectMapper.readValue(record.payload(), EventMessage.class);

                eventProducer.publishAndWait(event);

                jdbcTemplate.update("""
                        UPDATE ingestion_outbox
                        SET status = 'PUBLISHED',
                            published_at = CURRENT_TIMESTAMP,
                            last_error = NULL
                        WHERE event_id = ? AND status = 'PENDING'
                        """, record.eventId());


            } catch (Exception e) {
                String message = e.getMessage() == null
                        ? e.getClass().getSimpleName()
                        : e.getMessage();
                if (message.length() > 2000) {
                    message = message.substring(0, 2000);
                }

                jdbcTemplate.update("""
                        UPDATE ingestion_outbox
                        SET attempts = attempts + 1,
                            last_error = ?
                        WHERE event_id = ? AND status = 'PENDING'
                        """, message, record.eventId());

                log.error("Outbox publication failed for eventId={}; it will be retried",
                        record.eventId(), e);
            }
        }
    }

    private record OutboxRecord(String eventId, String payload) {
    }
}
