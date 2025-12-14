#!/usr/bin/env python3
"""
Test script for Kafka Producer
"""

import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka_services.producers.fleet_producer import FleetKafkaProducer

def test_producer():
    """Test Kafka producer connection and message sending"""
    print("🧪 Testing Kafka Producer...")
    
    # Initialize producer
    try:
        producer = FleetKafkaProducer(bootstrap_servers='localhost:9092')
        print("✅ Producer connected to Kafka")
    except Exception as e:
        print(f"❌ Failed to connect: {e}")
        return False
    
    # Test telemetry message
    test_telemetry = {
        'vehicle_id': 'TEST_VEH001',
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'latitude': 24.8607,
        'longitude': 67.0011,
        'speed': 45.5,
        'fuel_level': 75.2,
        'engine_temp': 85.0
    }
    
    print("📤 Sending test telemetry message...")
    success = producer.send_telemetry(test_telemetry)
    
    if success:
        print("✅ Telemetry message sent successfully")
    else:
        print("❌ Failed to send telemetry message")
        return False
    
    # Test delivery message
    test_delivery = {
        'delivery_id': 'TEST_DEL001',
        'vehicle_id': 'TEST_VEH001',
        'driver_id': 'TEST_DRV001',
        'status': 'InProgress'
    }
    
    print("📤 Sending test delivery message...")
    success = producer.send_delivery(test_delivery)
    
    if success:
        print("✅ Delivery message sent successfully")
    else:
        print("❌ Failed to send delivery message")
        return False
    
    # Test incident message
    test_incident = {
        'incident_id': 'TEST_INC001',
        'vehicle_id': 'TEST_VEH001',
        'incident_type': 'harsh_braking',
        'severity': 'Medium'
    }
    
    print("📤 Sending test incident message...")
    success = producer.send_incident(test_incident)
    
    if success:
        print("✅ Incident message sent successfully")
    else:
        print("❌ Failed to send incident message")
        return False
    
    # Get stats
    stats = producer.get_stats()
    print(f"\n📊 Producer Stats: {stats}")
    
    # Close producer
    producer.close()
    print("\n✅ All tests passed!")
    return True

if __name__ == "__main__":
    success = test_producer()
    sys.exit(0 if success else 1)
