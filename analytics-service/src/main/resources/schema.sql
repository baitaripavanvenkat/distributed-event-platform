CREATE TABLE IF NOT EXISTS analytics_processed_events (
    event_id       VARCHAR(255) PRIMARY KEY,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hourly_event_metrics (
    metric_type    VARCHAR(100) NOT NULL,
    metric_key     VARCHAR(255) NOT NULL,
    window_start   TIMESTAMPTZ NOT NULL,
    window_end     TIMESTAMPTZ NOT NULL,
    metric_value   BIGINT NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_hourly_event_metric
        UNIQUE (metric_type, metric_key, window_start)
);

CREATE INDEX IF NOT EXISTS idx_hourly_event_metrics_window
    ON hourly_event_metrics (window_start DESC);
