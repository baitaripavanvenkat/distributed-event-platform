package com.pavan.processing.repository;

import com.pavan.processing.entity.EventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface EventRepository extends JpaRepository<EventEntity, Long> {

    Optional<EventEntity> findByEventId(String eventId);

    boolean existsByEventId(String eventId);

    @Modifying
    @Query(value = """
            INSERT INTO events (
                event_id,
                event_type,
                user_id,
                event_timestamp,
                source,
                payload,
                created_at
            )
            VALUES (
                :eventId,
                :eventType,
                :userId,
                :eventTimestamp,
                :source,
                CAST(:payload AS TEXT),
                :createdAt
            )
            ON CONFLICT (event_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(
            @Param("eventId") String eventId,
            @Param("eventType") String eventType,
            @Param("userId") String userId,
            @Param("eventTimestamp") java.time.Instant eventTimestamp,
            @Param("source") String source,
            @Param("payload") String payload,
            @Param("createdAt") java.time.Instant createdAt
    );
}