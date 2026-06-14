SELECT remove_retention_policy('event_metrics_1h', if_exists => TRUE);
SELECT remove_retention_policy('event_metrics_5m', if_exists => TRUE);
SELECT remove_retention_policy('event_metrics_1m', if_exists => TRUE);

DROP MATERIALIZED VIEW IF EXISTS event_metrics_1h;
DROP MATERIALIZED VIEW IF EXISTS event_metrics_5m;
DROP MATERIALIZED VIEW IF EXISTS event_metrics_1m;
