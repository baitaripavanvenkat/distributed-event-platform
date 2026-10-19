package com.pavan.processing.entity;

import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(
        name = "events",
        indexes = {
                @Index(
                        name = "idx_events_event_id",
                        columnList = "event_id"
                ),
                @Index(
                        name = "idx_events_user_id",
                        columnList = "user_id"
                ),
                @Index(
                        name = "idx_events_timestamp",
                        columnList = "event_timestamp"
                )
        }
)
public class EventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(
            name = "event_id",
            nullable = false,
            unique = true
    )
    private String eventId;

    @Column(
            name = "event_type",
            nullable = false
    )
    private String eventType;

    @Column(
            name = "user_id",
            nullable = false
    )
    private String userId;

    @Column(
            name = "event_timestamp",
            nullable = false
    )
    private Instant timestamp;

    @Column(
            name = "source",
            nullable = false
    )
    private String source;

    @Column(
            name = "payload",
            columnDefinition = "TEXT"
    )
    private String payload;

    @Column(
            name = "created_at",
            nullable = false
    )
    private Instant createdAt;

    public EventEntity() {
    }

    public Long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }
}