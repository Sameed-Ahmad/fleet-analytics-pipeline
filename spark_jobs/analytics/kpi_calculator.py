#!/usr/bin/env python3
"""
KPI Calculator - Calculates and caches 10+ fleet analytics KPIs
"""

from pymongo import MongoClient
import redis
import json
from datetime import datetime
import logging

class KPICalculator:
    """Calculate 10+ KPIs and cache in Redis"""
    
    def __init__(self):
        self.logger = self._setup_logger()
        self.mongo_client = MongoClient('mongodb://admin:admin123@localhost:27017/')
        self.db = self.mongo_client['fleet_analytics']
        self.redis_client = redis.Redis(
            host='localhost',
            port=6379,
            password='redis123',
            decode_responses=True
        )
        self.logger.info("Connections established")
    
    def _setup_logger(self):
        logger = logging.getLogger('KPICalculator')
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger
    
    def calculate_all_kpis(self):
        """Calculate all 12 KPIs"""
        self.logger.info("Calculating 12 KPIs...")
        
        kpis = {}
        
        # KPI 1: Total Fleet Size
        kpis['total_vehicles'] = self.db.dim_vehicles.count_documents({})
        
        # KPI 2: Active Vehicles (in telemetry)
        kpis['active_vehicles'] = len(self.db.telemetry_events.distinct('vehicle_id'))
        
        # KPI 3: Fleet Utilization %
        if kpis['total_vehicles'] > 0:
            kpis['fleet_utilization_percent'] = round(
                (kpis['active_vehicles'] / kpis['total_vehicles']) * 100, 2
            )
        else:
            kpis['fleet_utilization_percent'] = 0
        
        # KPI 4: Total Drivers
        kpis['total_drivers'] = self.db.dim_drivers.count_documents({})
        
        # KPI 5: Drivers On Route
        kpis['drivers_on_route'] = self.db.dim_drivers.count_documents({'status': 'OnRoute'})
        
        # KPI 6: Average Driver Rating
        pipeline = [{'$group': {'_id': None, 'avg_rating': {'$avg': '$rating'}}}]
        result = list(self.db.dim_drivers.aggregate(pipeline))
        kpis['avg_driver_rating'] = round(result[0]['avg_rating'], 2) if result else 0
        
        # KPI 7: Average Speed
        pipeline = [
            {'$match': {'speed': {'$gt': 0}}},
            {'$group': {'_id': None, 'avg_speed': {'$avg': '$speed'}}}
        ]
        result = list(self.db.telemetry_events.aggregate(pipeline))
        kpis['avg_speed_kmh'] = round(result[0]['avg_speed'], 2) if result else 0
        
        # KPI 8: Average Fuel Level
        pipeline = [{'$group': {'_id': None, 'avg_fuel': {'$avg': '$fuel_level'}}}]
        result = list(self.db.telemetry_events.aggregate(pipeline))
        kpis['avg_fuel_level_percent'] = round(result[0]['avg_fuel'], 2) if result else 0
        
        # KPI 9: Total Deliveries
        kpis['total_deliveries'] = self.db.deliveries.count_documents({})
        
        # KPI 10: Completed Deliveries
        kpis['completed_deliveries'] = self.db.deliveries.count_documents({'status': 'Completed'})
        
        # KPI 11: Delivery Success Rate %
        if kpis['total_deliveries'] > 0:
            kpis['delivery_success_rate_percent'] = round(
                (kpis['completed_deliveries'] / kpis['total_deliveries']) * 100, 2
            )
        else:
            kpis['delivery_success_rate_percent'] = 0
        
        # KPI 12: Total Incidents
        kpis['total_incidents'] = self.db.incidents.count_documents({})
        
        # KPI 13: High Severity Incidents
        kpis['high_severity_incidents'] = self.db.incidents.count_documents({'severity': 'High'})
        
        # KPI 14: Incident Rate (incidents per 1000 trips)
        telemetry_count = self.db.telemetry_events.count_documents({})
        if telemetry_count > 0:
            kpis['incident_rate_per_1000'] = round(
                (kpis['total_incidents'] / telemetry_count) * 1000, 2
            )
        else:
            kpis['incident_rate_per_1000'] = 0
        
        self.logger.info(f"Calculated {len(kpis)} KPIs")
        return kpis
    
    def cache_kpis(self, kpis):
        """Cache KPIs in Redis with 5-minute TTL"""
        self.logger.info("Caching KPIs in Redis...")
        
        for kpi_name, value in kpis.items():
            key = f"cache:kpi:{kpi_name}"
            data = {
                'value': value,
                'timestamp': datetime.utcnow().isoformat(),
                'ttl': 300
            }
            self.redis_client.setex(key, 300, json.dumps(data))
        
        self.logger.info(f"Cached {len(kpis)} KPIs (TTL: 5 minutes)")
    
    def get_cached_kpi(self, kpi_name):
        """Retrieve KPI from cache"""
        key = f"cache:kpi:{kpi_name}"
        data = self.redis_client.get(key)
        return json.loads(data) if data else None
    
    def print_kpis(self, kpis):
        """Print KPIs in formatted table"""
        print("\n" + "=" * 80)
        print("FLEET ANALYTICS KPIs")
        print("=" * 80)
        for kpi_name, value in kpis.items():
            formatted = kpi_name.replace('_', ' ').title()
            print(f"{formatted:.<60} {value}")
        print("=" * 80)
    
    def close(self):
        self.mongo_client.close()
        self.logger.info("Connections closed")


def main():
    print("=" * 80)
    print("KPI Calculator - Fleet Analytics")
    print("=" * 80)
    
    calculator = KPICalculator()
    
    try:
        # Calculate KPIs
        kpis = calculator.calculate_all_kpis()
        
        # Print KPIs
        calculator.print_kpis(kpis)
        
        # Cache in Redis
        calculator.cache_kpis(kpis)
        
        print("\n KPI calculation complete!")
        print(" KPIs cached in Redis for 5 minutes")
        
    finally:
        calculator.close()


if __name__ == "__main__":
    main()
