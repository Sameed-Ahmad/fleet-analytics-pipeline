"""
Fleet Analytics Data Generator with Kafka Integration
Generates realistic vehicle telemetry, deliveries, and incidents
Publishes data to Kafka topics in real-time
"""

import json
import random
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any
import uuid
import sys
import os

# Add parent directory to path to import kafka producer
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from kafka_services.producers.fleet_producer import FleetKafkaProducer
except ImportError:
    print(" Warning: FleetKafkaProducer not found. Running in test mode without Kafka.")
    FleetKafkaProducer = None


class FleetDataGenerator:
    """
    Generates realistic fleet data using statistical models
    NOTE: Phase 2 will implement proper statistical models (Markov, Gaussian, Poisson, AR(1), HMM)
    This is a Phase 4 placeholder for Kafka integration testing
    """
    
    def __init__(self, kafka_producer: 'FleetKafkaProducer' = None):
        """
        Initialize data generator
        
        Args:
            kafka_producer: FleetKafkaProducer instance (optional for testing)
        """
        self.kafka_producer = kafka_producer
        self.logger = self._setup_logger()
        
        # Fleet configuration
        self.num_vehicles = 100  # Will be 500 in final version
        self.num_drivers = 70    # Will be 350 in final version
        
        # Initialize fleet
        self.vehicles = self._init_vehicles()
        self.drivers = self._init_drivers()
        self.active_deliveries = {}
        
        # Statistics tracking
        self.stats = {
            'telemetry_sent': 0,
            'deliveries_sent': 0,
            'incidents_sent': 0,
            'errors': 0
        }
        
        self.logger.info(f"Fleet initialized: {self.num_vehicles} vehicles, {self.num_drivers} drivers")
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger('FleetDataGenerator')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _init_vehicles(self) -> List[Dict[str, Any]]:
        """Initialize vehicle fleet"""
        vehicle_types = ['Van', 'Truck', 'Pickup', 'SUV']
        makes = ['Ford', 'Mercedes', 'Toyota', 'Volvo', 'Isuzu']
        
        vehicles = []
        for i in range(self.num_vehicles):
            vehicle = {
                'vehicle_id': f'VEH{str(i+1).zfill(4)}',
                'vehicle_type': random.choice(vehicle_types),
                'make': random.choice(makes),
                'model': f'Model-{random.randint(1, 5)}',
                'year': random.randint(2018, 2024),
                'capacity_kg': random.randint(500, 3000),
                'fuel_capacity': random.randint(60, 120),
                # Current state
                'latitude': 24.8607 + random.uniform(-0.5, 0.5),
                'longitude': 67.0011 + random.uniform(-0.5, 0.5),
                'speed': 0,
                'fuel_level': random.uniform(30, 95),
                'engine_temp': random.uniform(80, 95),
                'odometer': random.randint(10000, 200000),
                'driver_id': None
            }
            vehicles.append(vehicle)
        
        return vehicles
    
    def _init_drivers(self) -> List[Dict[str, Any]]:
        """Initialize driver pool"""
        driver_states = ['Available', 'OnRoute', 'OnBreak', 'OffDuty']
        
        drivers = []
        for i in range(self.num_drivers):
            driver = {
                'driver_id': f'DRV{str(i+1).zfill(4)}',
                'name': f'Driver {i+1}',
                'license_number': f'LIC{random.randint(100000, 999999)}',
                'experience_years': random.randint(1, 20),
                'rating': round(random.uniform(3.5, 5.0), 2),
                'status': random.choice(driver_states),
                'current_vehicle': None
            }
            drivers.append(driver)
        
        return drivers
    
    def generate_telemetry(self) -> Dict[str, Any]:
        """
        Generate vehicle telemetry data
        NOTE: Will use Gaussian distribution for speed, AR(1) for engine temp in Phase 2
        """
        vehicle = random.choice(self.vehicles)
        
        # Simulate vehicle movement (Phase 2 will use Markov Chain routes)
        vehicle['latitude'] += random.uniform(-0.001, 0.001)
        vehicle['longitude'] += random.uniform(-0.001, 0.001)
        vehicle['speed'] = max(0, min(120, vehicle['speed'] + random.uniform(-5, 5)))
        vehicle['fuel_level'] = max(0, vehicle['fuel_level'] - random.uniform(0.01, 0.1))
        vehicle['engine_temp'] = max(70, min(110, vehicle['engine_temp'] + random.uniform(-2, 2)))
        vehicle['odometer'] += vehicle['speed'] * 0.001
        
        telemetry = {
            'vehicle_id': vehicle['vehicle_id'],
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'latitude': round(vehicle['latitude'], 6),
            'longitude': round(vehicle['longitude'], 6),
            'speed': round(vehicle['speed'], 2),
            'fuel_level': round(vehicle['fuel_level'], 2),
            'engine_temp': round(vehicle['engine_temp'], 2),
            'odometer': round(vehicle['odometer'], 1),
            'driver_id': vehicle['driver_id'],
            'status': 'Moving' if vehicle['speed'] > 5 else 'Idle'
        }
        
        return telemetry
    
    def generate_delivery(self) -> Dict[str, Any]:
        """
        Generate delivery event
        NOTE: Phase 2 will implement realistic route generation with Markov Chain
        """
        # Select available vehicle and driver
        available_vehicles = [v for v in self.vehicles if v['driver_id'] is None]
        available_drivers = [d for d in self.drivers if d['status'] == 'Available']
        
        if not available_vehicles or not available_drivers:
            return None
        
        vehicle = random.choice(available_vehicles)
        driver = random.choice(available_drivers)
        
        # Assign driver to vehicle
        vehicle['driver_id'] = driver['driver_id']
        driver['status'] = 'OnRoute'
        driver['current_vehicle'] = vehicle['vehicle_id']
        
        delivery_id = str(uuid.uuid4())
        
        delivery = {
            'delivery_id': delivery_id,
            'vehicle_id': vehicle['vehicle_id'],
            'driver_id': driver['driver_id'],
            'warehouse_id': f'WH{random.randint(1, 20):03d}',
            'customer_id': f'CUST{random.randint(1, 10000):05d}',
            'pickup_time': datetime.utcnow().isoformat() + 'Z',
            'estimated_delivery': (datetime.utcnow() + timedelta(minutes=random.randint(30, 120))).isoformat() + 'Z',
            'pickup_location': {
                'latitude': round(vehicle['latitude'], 6),
                'longitude': round(vehicle['longitude'], 6)
            },
            'delivery_location': {
                'latitude': round(vehicle['latitude'] + random.uniform(-0.1, 0.1), 6),
                'longitude': round(vehicle['longitude'] + random.uniform(-0.1, 0.1), 6)
            },
            'distance_km': round(random.uniform(5, 50), 2),
            'package_weight': round(random.uniform(1, 100), 2),
            'status': 'InProgress'
        }
        
        # Track active delivery
        self.active_deliveries[delivery_id] = {
            'vehicle_id': vehicle['vehicle_id'],
            'driver_id': driver['driver_id'],
            'start_time': datetime.utcnow()
        }
        
        return delivery
    
    def generate_incident(self) -> Dict[str, Any]:
        """
        Generate incident/event (harsh braking, speeding, etc.)
        NOTE: Phase 2 will use Poisson distribution and HMM for driver behavior
        """
        # Only generate incidents for vehicles in motion
        moving_vehicles = [v for v in self.vehicles if v['speed'] > 10]
        
        if not moving_vehicles:
            return None
        
        vehicle = random.choice(moving_vehicles)
        
        incident_types = [
            'harsh_braking',
            'harsh_acceleration',
            'speeding',
            'sharp_turn',
            'idling_excessive'
        ]
        
        incident = {
            'incident_id': str(uuid.uuid4()),
            'vehicle_id': vehicle['vehicle_id'],
            'driver_id': vehicle['driver_id'],
            'incident_type': random.choice(incident_types),
            'severity': random.choice(['Low', 'Medium', 'High']),
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'latitude': round(vehicle['latitude'], 6),
            'longitude': round(vehicle['longitude'], 6),
            'speed_at_incident': round(vehicle['speed'], 2),
            'description': 'Auto-generated incident event'
        }
        
        return incident
    
    def complete_random_deliveries(self):
        """Randomly complete some active deliveries"""
        to_complete = []
        
        for delivery_id, info in self.active_deliveries.items():
            # 5% chance to complete delivery on each cycle
            if random.random() < 0.05:
                to_complete.append(delivery_id)
        
        for delivery_id in to_complete:
            info = self.active_deliveries[delivery_id]
            
            # Release vehicle and driver
            for vehicle in self.vehicles:
                if vehicle['vehicle_id'] == info['vehicle_id']:
                    vehicle['driver_id'] = None
            
            for driver in self.drivers:
                if driver['driver_id'] == info['driver_id']:
                    driver['status'] = 'Available'
                    driver['current_vehicle'] = None
            
            # Send completion event
            completion = {
                'delivery_id': delivery_id,
                'vehicle_id': info['vehicle_id'],
                'driver_id': info['driver_id'],
                'completion_time': datetime.utcnow().isoformat() + 'Z',
                'status': 'Completed',
                'duration_minutes': (datetime.utcnow() - info['start_time']).seconds // 60
            }
            
            if self.kafka_producer:
                self.kafka_producer.send_delivery(completion)
            
            del self.active_deliveries[delivery_id]
    
    def run_generation_cycle(self):
        """Run one cycle of data generation"""
        # Generate telemetry for all vehicles (every cycle)
        for _ in range(self.num_vehicles):
            telemetry = self.generate_telemetry()
            if self.kafka_producer:
                success = self.kafka_producer.send_telemetry(telemetry)
                if success:
                    self.stats['telemetry_sent'] += 1
                else:
                    self.stats['errors'] += 1
        
        # Generate new deliveries (10% chance per cycle)
        if random.random() < 0.1:
            delivery = self.generate_delivery()
            if delivery and self.kafka_producer:
                success = self.kafka_producer.send_delivery(delivery)
                if success:
                    self.stats['deliveries_sent'] += 1
                else:
                    self.stats['errors'] += 1
        
        # Generate incidents (5% chance per cycle)
        if random.random() < 0.05:
            incident = self.generate_incident()
            if incident and self.kafka_producer:
                success = self.kafka_producer.send_incident(incident)
                if success:
                    self.stats['incidents_sent'] += 1
                else:
                    self.stats['errors'] += 1
        
        # Complete some deliveries
        self.complete_random_deliveries()
    
    def run_continuous(self, interval_seconds: int = 3):
        """
        Run continuous data generation
        
        Args:
            interval_seconds: Time between generation cycles
        """
        self.logger.info(f"Starting continuous data generation (interval: {interval_seconds}s)")
        self.logger.info(f"   Press Ctrl+C to stop")
        
        try:
            cycle = 0
            while True:
                cycle += 1
                self.run_generation_cycle()
                
                # Print stats every 10 cycles
                if cycle % 10 == 0:
                    self.logger.info(
                        f"Cycle {cycle} | Telemetry: {self.stats['telemetry_sent']}, "
                        f"Deliveries: {self.stats['deliveries_sent']}, "
                        f"Incidents: {self.stats['incidents_sent']}, "
                        f"Errors: {self.stats['errors']}"
                    )
                
                time.sleep(interval_seconds)
        
        except KeyboardInterrupt:
            self.logger.info("\n Generation stopped by user")
            self._print_final_stats()
    
    def _print_final_stats(self):
        """Print final generation statistics"""
        print(f"\n{'='*80}")
        print("GENERATION STATISTICS")
        print(f"{'='*80}")
        print(f"Telemetry events: {self.stats['telemetry_sent']}")
        print(f"Delivery events:  {self.stats['deliveries_sent']}")
        print(f"Incident events:  {self.stats['incidents_sent']}")
        print(f"Errors:           {self.stats['errors']}")
        print(f"Total sent:       {sum(v for k, v in self.stats.items() if k != 'errors')}")


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Fleet Data Generator with Kafka')
    parser.add_argument(
        '--kafka-servers',
        default='localhost:9092',
        help='Kafka bootstrap servers'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=3,
        help='Generation interval in seconds'
    )
    parser.add_argument(
        '--test-mode',
        action='store_true',
        help='Run without Kafka (testing only)'
    )
    
    args = parser.parse_args()
    
    # Initialize Kafka producer
    kafka_producer = None
    if not args.test_mode:
        if FleetKafkaProducer is None:
            print("Kafka producer not available. Run with --test-mode or fix imports.")
            sys.exit(1)
        
        kafka_producer = FleetKafkaProducer(bootstrap_servers=args.kafka_servers)
    else:
        print("Running in test mode (no Kafka)")
    
    # Create generator
    generator = FleetDataGenerator(kafka_producer=kafka_producer)
    
    try:
        # Run continuous generation
        generator.run_continuous(interval_seconds=args.interval)
    finally:
        if kafka_producer:
            kafka_producer.close()


if __name__ == "__main__":
    main()
