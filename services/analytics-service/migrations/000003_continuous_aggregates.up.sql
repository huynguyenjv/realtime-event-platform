-- 1-minute aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS event_metrics_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', timestamp) AS bucket,
    event_type,
    COUNT(*) AS event_count,
    AVG((payload->>'value')::numeric) AS avg_value,
    MIN((payload->>'value')::numeric) AS min_value,
    MAX((payload->>'value')::numeric) AS max_value
FROM events
GROUP BY bucket, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('event_metrics_1m',
    start_offset => INTERVAL '5 minutes',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE);

-- 5-minute aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS event_metrics_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', timestamp) AS bucket,
    event_type,
    COUNT(*) AS event_count,
    AVG((payload->>'value')::numeric) AS avg_value,
    MIN((payload->>'value')::numeric) AS min_value,
    MAX((payload->>'value')::numeric) AS max_value
FROM events
GROUP BY bucket, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('event_metrics_5m',
    start_offset => INTERVAL '30 minutes',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE);

-- 1-hour aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS event_metrics_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', timestamp) AS bucket,
    event_type,
    COUNT(*) AS event_count,
    AVG((payload->>'value')::numeric) AS avg_value,
    MIN((payload->>'value')::numeric) AS min_value,
    MAX((payload->>'value')::numeric) AS max_value
FROM events
GROUP BY bucket, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('event_metrics_1h',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists => TRUE);

-- Retention policies for aggregates
SELECT add_retention_policy('event_metrics_1m', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('event_metrics_5m', INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_retention_policy('event_metrics_1h', INTERVAL '365 days', if_not_exists => TRUE);
