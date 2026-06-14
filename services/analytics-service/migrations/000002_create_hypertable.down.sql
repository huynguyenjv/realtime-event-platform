SELECT remove_retention_policy('events', if_exists => TRUE);
ALTER TABLE events DROP COLUMN IF EXISTS processed_at;
ALTER TABLE events DROP COLUMN IF EXISTS event_size_bytes;
