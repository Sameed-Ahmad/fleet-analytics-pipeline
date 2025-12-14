#!/bin/bash
# Start test consumer

cd "$(dirname "$0")/.."
source venv/bin/activate

echo "🎧 Starting Kafka Consumer..."
python kafka_services/consumers/test_consumer.py --mode consume
