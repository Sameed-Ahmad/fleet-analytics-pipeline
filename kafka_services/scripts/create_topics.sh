#!/bin/bash
echo "Creating Kafka topics..."

docker exec fleet-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic vehicle-telemetry \
  --partitions 8 \
  --replication-factor 1 \
  --if-not-exists || true

docker exec fleet-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic deliveries \
  --partitions 4 \
  --replication-factor 1 \
  --if-not-exists || true

docker exec fleet-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic incidents \
  --partitions 4 \
  --replication-factor 1 \
  --if-not-exists || true

echo "✓ Topics created!"
docker exec fleet-kafka kafka-topics --list --bootstrap-server localhost:9092
