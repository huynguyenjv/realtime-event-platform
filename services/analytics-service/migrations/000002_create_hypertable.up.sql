-- Add columns needed by analytics service
ALTER TABLE events ADD COLUMN IF NOT EXISTS processed_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE events ADD COLUMN IF NOT EXISTS event_size_bytes INTEGER;

-- Convert events table to hypertable
SELECT create_hypertable('events', 'timestamp', if_not_exists => TRUE, migrate_data => TRUE);

-- Retention policy: drop raw events older than 30 days
SELECT add_retention_policy('events', INTERVAL '30 days', if_not_exists => TRUE);
