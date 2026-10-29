package com.pavan.ingestion.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pavan.common.event.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final long BATCH_ACK_TIMEOUT_SECONDS = 15;

    private final JdbcTemplate jdbcTemplate;
    private final EventProducer eventProducer;
    private final ObjectMapper objectMapper;
    private final int batchSize;

    public OutboxPublisher(
            JdbcTemplate jdbcTemplate,
            EventProducer eventProducer,
            ObjectMapper outboxObjectMapper,
            @Value("${outbox.publisher.batch-size:250}") int batchSize) {
        this.jdbcTemplate = jdbcTemplate;
        this.eventProducer = eventProducer;
        this.objectMapper = outboxObjectMapper;
        this.batchSize = Math.max(1, batchSize);
    }

    /**
     * Sends each selected batch concurrently, waits for broker acknowledgements,
     * and only then marks successful records as published. A timed-out send is
     * treated as retryable; it may have reached Kafka, so downstream consumers
     * must remain idempotent (as they already are in this project).
     */
    @Scheduled(fixedDelayString = "${outbox.publisher.fixed-delay-ms:50}")
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

        if (pending.isEmpty()) {
            return;
        }

        List<Publication> publications = new ArrayList<>(pending.size());
        List<FailedPublication> failures = new ArrayList<>();

        // Start all sends first. KafkaTemplate returns immediately with a future.
        for (OutboxRecord record : pending) {
            try {
                EventMessage event = objectMapper.readValue(record.payload(), EventMessage.class);
                CompletableFuture<SendResult<String, EventMessage>> future =
                        eventProducer.publishAsync(event);
                publications.add(new Publication(record.eventId(), future));
            } catch (Exception e) {
                failures.add(new FailedPublication(record.eventId(), errorMessage(e)));
                log.error("Could not start publication for eventId={}; it will be retried",
                        record.eventId(), e);
            }
        }

        // Wait for the concurrent batch with one shared timeout, not one timeout per event.
        CompletableFuture<?>[] futures = publications.stream()
                .map(Publication::future)
                .toArray(CompletableFuture[]::new);
        if (futures.length > 0) {
            try {
                CompletableFuture.allOf(futures)
                        .get(BATCH_ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                // Inspect individual futures below; completed successes are still recorded.
                log.warn("Timed out waiting for Kafka acknowledgements for an outbox batch of {} events",
                        publications.size());
            } catch (Exception e) {
                // allOf fails if any send fails. Inspect each future individually below.
                log.debug("One or more Kafka sends failed in the current outbox batch", e);
            }
        }

        List<String> publishedIds = new ArrayList<>();
        for (Publication publication : publications) {
            CompletableFuture<SendResult<String, EventMessage>> future = publication.future();
            if (!future.isDone()) {
                failures.add(new FailedPublication(publication.eventId(),
                        "Timed out waiting for Kafka acknowledgement; publication will be retried"));
                continue;
            }

            try {
                // The future is already complete, so this does not perform a blocking broker wait.
                future.join();
                publishedIds.add(publication.eventId());
            } catch (Exception e) {
                failures.add(new FailedPublication(publication.eventId(), errorMessage(e)));
                log.error("Kafka publication failed for eventId={}; it will be retried",
                        publication.eventId(), e);
            }
        }

        if (!publishedIds.isEmpty()) {
            jdbcTemplate.batchUpdate(
                    """
                    UPDATE ingestion_outbox
                    SET status = 'PUBLISHED',
                        published_at = CURRENT_TIMESTAMP,
                        last_error = NULL
                    WHERE event_id = ? AND status = 'PENDING'
                    """,
                    publishedIds,
                    publishedIds.size(),
                    (ps, eventId) -> ps.setString(1, eventId)
            );
        }

        if (!failures.isEmpty()) {
            jdbcTemplate.batchUpdate(
                    """
                    UPDATE ingestion_outbox
                    SET attempts = attempts + 1,
                        last_error = ?
                    WHERE event_id = ? AND status = 'PENDING'
                    """,
                    failures,
                    failures.size(),
                    (ps, failure) -> {
                        ps.setString(1, truncate(failure.message(), 2000));
                        ps.setString(2, failure.eventId());
                    }
            );
        }

        log.debug("Outbox batch complete: selected={}, published={}, retryableFailures={}",
                pending.size(), publishedIds.size(), failures.size());
    }

    private static String errorMessage(Throwable error) {
        Throwable cause = error;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        String message = cause.getMessage();
        return truncate(message == null ? cause.getClass().getSimpleName() : message, 2000);
    }

    private static String truncate(String value, int maxLength) {
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }

    private record OutboxRecord(String eventId, String payload) {}
    private record Publication(
            String eventId,
            CompletableFuture<SendResult<String, EventMessage>> future) {}
    private record FailedPublication(String eventId, String message) {}
}
