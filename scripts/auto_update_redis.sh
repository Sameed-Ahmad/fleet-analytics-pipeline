#!/bin/bash
cd ~/fleet-analytics-pipeline
source venv/bin/activate

echo " Starting Redis Auto-Update Loop"
echo "Updates every 60 seconds to match Grafana refresh"
echo "Press Ctrl+C to stop"
echo ""

while true; do
    echo " $(date '+%H:%M:%S') - Updating cache..."
    python scripts/mongodb_to_redis.py 2>&1 | grep -E "(✓|⚠|Total|Average)"
    echo " Update complete. Next update in 60 seconds..."
    echo ""
    sleep 60
done
