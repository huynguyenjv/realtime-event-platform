#!/bin/bash

KAFKA_CONTAINER="event-platform-kafka"
BOOTSTRAP_SERVER="localhost:9092"

TOPICS=(
    "events.raw"
    "events.processed"
    "events.predictions"
    "events.alerts"
    "events.analytics"
)

echo "Creating Kafka topics..."

for topic in "${TOPICS[@]}"; do
    echo "Creating topic: $topic"
    docker exec $KAFKA_CONTAINER kafka-topics --create \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --replication-factor 1 \
        --partitions 3 \
        --topic $topic \
        --if-not-exists
done

echo ""
echo "Listing all topics:"
docker exec $KAFKA_CONTAINER kafka-topics --list --bootstrap-server $BOOTSTRAP_SERVER
