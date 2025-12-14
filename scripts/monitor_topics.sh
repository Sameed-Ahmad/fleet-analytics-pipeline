#!/bin/bash
# Monitor Kafka topics

echo "📊 Kafka Topic Statistics"
echo "=========================="

for topic in vehicle-telemetry deliveries incidents; do
    count=$(docker exec fleet-kafka kafka-run-class kafka.tools.GetOffsetShell \
      --broker-list localhost:9092 \
      --topic $topic \
      | awk -F: '{sum += $3} END {print sum}')
    
    echo "$topic: $count messages"
done
