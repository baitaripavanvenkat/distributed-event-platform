CREATE TABLE IF NOT EXISTS ingestion_outbox (
                                                event_id VARCHAR(255) PRIMARY KEY,
    payload TEXT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMPTZ
    );

CREATE INDEX IF NOT EXISTS idx_ingestion_outbox_pending
    ON ingestion_outbox (created_at)
    WHERE status = 'PENDING';