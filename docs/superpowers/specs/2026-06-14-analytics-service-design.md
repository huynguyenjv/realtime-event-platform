# Analytics Service — Design Spec

## Overview

Analytics Service is the core stream-processing service in the Realtime Event Platform. It consumes raw events from Kafka, processes them through a multi-stage pipeline, computes real-time and historical aggregations, and publishes processed results for downstream services.

## Architecture

```
events.raw (Kafka)
    │
    ▼
┌─────────────────────────────────────────────────┐
│              ANALYTICS SERVICE                   │
│                                                  │
│  Consumer Group ──► Pipeline:                    │
│                                                  │
│    Validate ──► Transform ──► Fan-out:           │
│                                  │               │
│                          ┌───────┼────────┐      │
│                          ▼       ▼        ▼      │
│                     Aggregate  Store   Publish    │
│                     (memory)   (DB)    (Kafka)    │
│                        │                          │
│                        ▼                          │
│                      Redis                        │
│                   (real-time)                     │
│                                                  │
│  HTTP REST API:                                  │
│    GET /api/v1/metrics/realtime   ← Redis        │
│    GET /api/v1/metrics/history    ← TimescaleDB  │
│    GET /api/v1/stats/:event_type                 │
│    GET /health, /ready                           │
└─────────────────────────────────────────────────┘
```

## Data Flow

1. **Ingest**: Kafka consumer group reads from `events.raw` topic
2. **Validate**: Check event structure — drop malformed events, log errors
3. **Transform**: Normalize payload, enrich metadata (add processing timestamp, compute derived fields)
4. **Fan-out** (parallel):
   - **Aggregate**: Update in-memory time-window counters (1m, 5m, 1h), flush to Redis every 10 seconds
   - **Store**: Batch insert into TimescaleDB hypertable (batch size: 100, flush interval: 5s)
   - **Publish**: Produce to `events.processed` (normalized event) and `events.analytics` (aggregation snapshots)

## Pipeline Design

Channel-based pipeline using Go channels. Each stage is a goroutine reading from an input channel and writing to an output channel.

```go
rawCh      → validateStage → validCh
validCh    → transformStage → transformedCh
transformedCh → fanoutStage → (aggregateCh, storeCh, publishCh)
```

### Stage Details

**Validate Stage**
- Required fields: `id`, `type`, `source`, `timestamp`
- Payload must be valid JSON object
- Drop events older than 24 hours (configurable)
- Emit metric: `events_validated_total{status="valid|invalid"}`

**Transform Stage**
- Add `processed_at` timestamp
- Normalize `event_type` to lowercase with dot notation
- Extract numeric values from payload for statistical aggregation
- Compute `event_size_bytes` from serialized payload

**Fan-out Stage**
- Non-blocking sends to all three downstream channels
- If any downstream channel is full, log warning and skip (no backpressure propagation to avoid pipeline stall)

## Storage

### TimescaleDB

**Hypertable: `events`** (extends existing migration)

Uses the existing `events` table from `000001_init_schema.up.sql` but converts it to a TimescaleDB hypertable. The existing schema has:
- `id` UUID PK
- `event_type` VARCHAR(100)
- `source` VARCHAR(255)
- `payload` JSONB
- `metadata` JSONB
- `timestamp` TIMESTAMPTZ (indexed)
- `created_at` TIMESTAMPTZ

Additional columns to add:
- `processed_at` TIMESTAMPTZ — when the analytics service processed it
- `event_size_bytes` INTEGER — serialized payload size

TimescaleDB conversion:
```sql
SELECT create_hypertable('events', 'timestamp', migrate_data => true);
```

**Continuous Aggregates:**

```sql
-- 1-minute aggregation
CREATE MATERIALIZED VIEW event_metrics_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', timestamp) AS bucket,
    event_type,
    COUNT(*) AS event_count,
    AVG((payload->>'value')::numeric) AS avg_value,
    MIN((payload->>'value')::numeric) AS min_value,
    MAX((payload->>'value')::numeric) AS max_value,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (payload->>'value')::numeric) AS p50_value,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY (payload->>'value')::numeric) AS p95_value,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY (payload->>'value')::numeric) AS p99_value
FROM events
WHERE payload ? 'value'
GROUP BY bucket, event_type;

-- 5-minute and 1-hour aggregations follow the same pattern
```

**Refresh policies:**
- 1m aggregate: refresh every 1 minute, covers last 5 minutes
- 5m aggregate: refresh every 5 minutes, covers last 30 minutes
- 1h aggregate: refresh every 30 minutes, covers last 3 hours

**Retention policy:**
- Raw events: 30 days
- 1m aggregates: 7 days
- 5m aggregates: 30 days
- 1h aggregates: 365 days

### Redis

**Keys and structures:**

| Key Pattern | Type | TTL | Purpose |
|---|---|---|---|
| `metrics:realtime:{event_type}:count` | String (counter) | 5m | Current window event count |
| `metrics:realtime:{event_type}:rate` | String | 5m | Events per second |
| `metrics:realtime:top_types` | Sorted Set | 5m | Top event types by count |
| `metrics:realtime:total_count` | String (counter) | 5m | Total events across all types |
| `metrics:window:{window}:{event_type}` | Hash | window duration + 1m | Aggregated stats per window (count, sum, min, max) |

**Flush strategy:**
- Every 10 seconds, the aggregator flushes in-memory counters to Redis using MULTI/EXEC pipeline
- Redis keys have TTLs matching their window duration + buffer
- INCRBY for counters, ZADD for sorted sets

## Kafka Publishing

**Topic: `events.processed`**
- Content: Normalized, validated event with enriched metadata
- Key: `event.id`
- Consumers: Prediction Service, Alert Service, WebSocket Gateway

**Topic: `events.analytics`**
- Content: Aggregation snapshots (periodic, every 30 seconds)
- Key: `event_type`
- Format:
```json
{
  "event_type": "user.click",
  "window": "1m",
  "timestamp": "2026-06-14T10:00:00Z",
  "count": 1523,
  "rate_per_sec": 25.38,
  "avg_value": 42.5,
  "p95_value": 89.2,
  "p99_value": 97.1
}
```
- Consumers: WebSocket Gateway, Alert Service

## HTTP API

### Endpoints

**GET /api/v1/metrics/realtime**
- Source: Redis
- Response: Current counts, rates, top event types
- Latency target: < 10ms

```json
{
  "total_events": 125000,
  "events_per_second": 420.5,
  "top_event_types": [
    {"type": "user.click", "count": 45000, "rate": 150.2},
    {"type": "order.created", "count": 23000, "rate": 76.8}
  ],
  "window": "1m",
  "timestamp": "2026-06-14T10:05:00Z"
}
```

**GET /api/v1/metrics/history?from=&to=&interval=&event_type=**
- Source: TimescaleDB continuous aggregates
- Query params:
  - `from` (required): ISO 8601 timestamp
  - `to` (required): ISO 8601 timestamp
  - `interval`: `1m`, `5m`, `1h` (default: auto-select based on time range)
  - `event_type` (optional): filter by type
- Auto-select interval: < 1h → 1m, < 24h → 5m, > 24h → 1h

```json
{
  "interval": "5m",
  "from": "2026-06-14T09:00:00Z",
  "to": "2026-06-14T10:00:00Z",
  "data": [
    {
      "bucket": "2026-06-14T09:00:00Z",
      "event_type": "user.click",
      "count": 7500,
      "avg_value": 42.3,
      "min_value": 1.0,
      "max_value": 99.8,
      "p50_value": 40.1,
      "p95_value": 88.7,
      "p99_value": 96.5
    }
  ]
}
```

**GET /api/v1/stats/:event_type**
- Source: Redis (recent) + TimescaleDB (historical)
- Response: Detailed statistics for a specific event type

```json
{
  "event_type": "user.click",
  "realtime": {
    "count_1m": 150,
    "count_5m": 720,
    "count_1h": 8500,
    "rate_per_sec": 2.5
  },
  "today": {
    "total_count": 125000,
    "avg_value": 42.3,
    "p95_value": 88.7,
    "p99_value": 96.5
  }
}
```

**GET /health** — Service health status
**GET /ready** — Readiness check (Kafka + TimescaleDB + Redis connectivity)

## Configuration

```go
type Config struct {
    Port            string        // default: "8080"
    AppEnv          string        // default: "development"

    // Kafka
    KafkaBrokers    []string      // default: ["localhost:9092"]
    KafkaGroupID    string        // default: "analytics-service"
    ConsumeTopic    string        // default: "events.raw"
    ProducedTopics  []string      // ["events.processed", "events.analytics"]

    // TimescaleDB
    DatabaseURL     string        // default: "postgres://user:pass@localhost:5433/eventdb"

    // Redis
    RedisURL        string        // default: "redis://localhost:6379"

    // Pipeline
    WorkerCount     int           // default: 4
    ChannelBuffer   int           // default: 1000
    BatchSize       int           // default: 100
    BatchFlushInterval time.Duration // default: 5s

    // Aggregation
    FlushInterval   time.Duration // default: 10s
    MaxEventAge     time.Duration // default: 24h

    // Retention
    RawRetention    time.Duration // default: 30 days
    M1Retention     time.Duration // default: 7 days
    M5Retention     time.Duration // default: 30 days
    H1Retention     time.Duration // default: 365 days
}
```

## Project Structure

```
services/analytics-service/
├── cmd/server/main.go                 # Entry point, wiring, graceful shutdown
├── internal/
│   ├── config/config.go               # Environment-based config loading
│   ├── consumer/consumer.go           # Kafka consumer group wrapper
│   ├── pipeline/
│   │   ├── pipeline.go                # Pipeline orchestrator — wires stages together
│   │   ├── validator.go               # Stage 1: event validation
│   │   ├── transformer.go             # Stage 2: normalization + enrichment
│   │   └── fanout.go                  # Stage 3: parallel dispatch
│   ├── aggregator/
│   │   ├── window.go                  # In-memory time-window counters (sync.Map + atomic)
│   │   └── flusher.go                 # Periodic flush to Redis
│   ├── store/
│   │   └── timescaledb.go             # Batch insert + query methods
│   ├── publisher/publisher.go         # Kafka producer for processed + analytics topics
│   ├── handler/
│   │   ├── handler.go                 # Route registration
│   │   ├── metrics_handler.go         # GET /metrics/realtime, /metrics/history, /stats/:type
│   │   └── health.go                  # GET /health, /ready
│   ├── model/
│   │   ├── event.go                   # Domain models (ProcessedEvent, AggregationSnapshot)
│   │   └── metrics.go                 # API response models
│   └── cache/redis.go                 # Redis read/write operations
├── migrations/
│   ├── 000001_create_hypertable.up.sql
│   ├── 000001_create_hypertable.down.sql
│   ├── 000002_continuous_aggregates.up.sql
│   └── 000002_continuous_aggregates.down.sql
├── Dockerfile
└── go.mod
```

## Error Handling

- **Malformed events**: Log + increment counter, do not retry
- **TimescaleDB unavailable**: Buffer in memory (up to 10,000 events), retry with exponential backoff
- **Redis unavailable**: Skip cache update, serve stale data, log warning — not a fatal error
- **Kafka producer failure**: Retry 3 times with backoff, log error, do not block pipeline
- **Pipeline channel full**: Log warning, drop event, increment `events_dropped_total` counter

## Health & Readiness

- `/health`: Returns 200 if the service process is running
- `/ready`: Returns 200 only if all dependencies are reachable:
  - Kafka consumer is connected and consuming
  - TimescaleDB connection pool is healthy
  - Redis is pingable

## Metrics (Prometheus)

Expose on `/metrics` endpoint:
- `analytics_events_consumed_total` — events read from Kafka
- `analytics_events_validated_total{status}` — valid/invalid counts
- `analytics_events_stored_total` — events inserted to TimescaleDB
- `analytics_events_published_total{topic}` — events published per topic
- `analytics_events_dropped_total{reason}` — dropped events by reason
- `analytics_pipeline_latency_seconds` — end-to-end pipeline processing time
- `analytics_batch_size` — histogram of batch insert sizes
- `analytics_redis_flush_duration_seconds` — Redis flush timing

## Testing Strategy

- **Unit tests**: Each pipeline stage in isolation (validator, transformer, fanout)
- **Integration tests**: Pipeline end-to-end with embedded Kafka + PostgreSQL (testcontainers)
- **Benchmark tests**: Pipeline throughput under load

## Dependencies

```
github.com/gin-gonic/gin          — HTTP framework
github.com/segmentio/kafka-go     — Kafka client (consistent with collector-service)
github.com/jackc/pgx/v5           — PostgreSQL driver (native, better for TimescaleDB than GORM)
github.com/redis/go-redis/v9      — Redis client
github.com/prometheus/client_golang — Metrics
```

Note: Using `pgx` instead of GORM for TimescaleDB because:
- Direct SQL control for hypertables and continuous aggregates
- Better performance for batch inserts (COPY protocol)
- Native support for PostgreSQL-specific features
