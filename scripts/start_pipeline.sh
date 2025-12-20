#!/bin/bash

echo " Starting Fleet Analytics Pipeline"
echo "===================================="
echo ""

# Start MongoDB consumer in background
cd ~/fleet-analytics-pipeline
source venv/bin/activate

echo " Starting MongoDB Consumer..."
python kafka_services/consumers/mongodb_consumer.py &
CONSUMER_PID=$!
echo "   PID: $CONSUMER_PID"

sleep 3

echo " Starting Data Generator..."
python data_generators/fleet_generator_kafka.py --interval 3 &
GENERATOR_PID=$!
echo "   PID: $GENERATOR_PID"

sleep 3

echo " Starting Redis Cache Updater..."
./scripts/auto_update_redis.sh &
UPDATER_PID=$!
echo "   PID: $UPDATER_PID"

echo ""
echo " All components started!"
echo ""
echo " Open Grafana: http://localhost:3000"
echo ""
echo " To stop everything, press Ctrl+C"
echo ""

# Wait for Ctrl+C
trap "kill $CONSUMER_PID $GENERATOR_PID $UPDATER_PID; echo ''; echo ' Pipeline stopped'; exit" INT TERM

wait
