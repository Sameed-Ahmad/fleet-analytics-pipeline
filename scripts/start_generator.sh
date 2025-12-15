#!/bin/bash
# Start data generator

cd "$(dirname "$0")/.."
source venv/bin/activate

echo "Starting Fleet Data Generator..."
python data_generators/fleet_generator_kafka.py --interval 3
