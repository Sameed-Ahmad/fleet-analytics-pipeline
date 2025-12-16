#!/usr/bin/env python3
"""
Redis Staging Area for ETL Operations
Extracts data from MongoDB, stages in Redis for cleaning/transformation
"""

import redis
import json
import logging
from pymongo import MongoClient
from datetime import datetime, timedelta
from typing import Dict, List, Any

class RedisETLStaging:
    """
    Manages ETL staging area using Redis
    DUAL PURPOSE: (1) Cache for performance, (2) Staging for ETL operations
    """
    
    def __init__(
        self,
        mongo_uri='mongodb://admin:admin123@localhost:27017/',
        redis_host='localhost',
        redis_port=6379,
        redis_password='redis123'
    ):
        """
        Initialize connections
        
        Args:
            mongo_uri: MongoDB connection string
            redis_host: Redis host
            redis_port: Redis port
            redis_password: Redis password
        """
        self.mongo_client = MongoClient(mongo_uri)
        self.db = self.mongo_client['fleet_analytics']
        
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            decode_responses=True
        )
        
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging"""
        logger = logging.getLogger('RedisETLStaging')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def extract_and_stage_dimensions(self):
        """
        Extract dimension data from MongoDB and stage in Redis
        PURPOSE: ETL Staging - Clean and prepare dimension data
        """
        self.logger.info("STEP 1: EXTRACT from MongoDB")
        
        collections = {
            'vehicles': 'dim_vehicles',
            'drivers': 'dim_drivers',
            'warehouses': 'dim_warehouses',
            'customers': 'dim_customers'
        }
        
        for key, collection_name in collections.items():
            self.logger.info(f"   Extracting {collection_name}...")
            
            # Extract from MongoDB
            docs = list(self.db[collection_name].find({}, {'_id': 0}))
            
            self.logger.info(f"STEP 2: CLEAN data in Redis staging area")
            
            # Stage in Redis (ETL staging area)
            for doc in docs:
                # Clean: Convert datetime to string for JSON serialization
                for field, value in doc.items():
                    if isinstance(value, datetime):
                        doc[field] = value.isoformat()
                
                # Stage in Redis with key pattern: staging:dimension:id
                key_name = f"staging:{key}:{doc.get(f'{key[:-1]}_id' if key.endswith('s') else f'{key}_id')}"
                self.redis_client.setex(
                    key_name,
                    3600,  # 1 hour TTL for staging
                    json.dumps(doc)
                )
            
            self.logger.info(f"Staged {len(docs)} {key} in Redis")
        
        self.logger.info("All dimensions extracted and staged in Redis")
    
    def extract_and_stage_facts(self, hours_back: int = 1):
        """
        Extract recent fact data from MongoDB and stage in Redis
        PURPOSE: ETL Staging - Prepare fact data for analytics
        
        Args:
            hours_back: How many hours of data to extract
        """
        self.logger.info(f"STEP 1: EXTRACT fact data (last {hours_back} hours)")
        
        cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)
        
        # Extract telemetry events
        self.logger.info("   Extracting telemetry_events...")
        telemetry = list(self.db.telemetry_events.find(
            {'timestamp': {'$gte': cutoff_time}},
            {'_id': 0}
        ))
        
        self.logger.info("STEP 2: CLEAN fact data in Redis staging area")
        
        # Stage in Redis
        for event in telemetry:
            # Clean: Convert datetime to string
            if 'timestamp' in event and isinstance(event['timestamp'], datetime):
                event['timestamp'] = event['timestamp'].isoformat()
            
            # Stage with key pattern: staging:telemetry:vehicle_id:timestamp
            key_name = f"staging:telemetry:{event.get('vehicle_id')}:{event.get('timestamp')}"
            self.redis_client.setex(
                key_name,
                1800,  # 30 minutes TTL for fact data
                json.dumps(event)
            )
        
        self.logger.info(f"Staged {len(telemetry)} telemetry events in Redis")
        
        # Extract deliveries
        self.logger.info("   Extracting deliveries...")
        deliveries = list(self.db.deliveries.find({}, {'_id': 0}).limit(1000))
        
        for delivery in deliveries:
            # Clean timestamps
            for field in ['pickup_time', 'estimated_delivery', 'completion_time']:
                if field in delivery and isinstance(delivery[field], datetime):
                    delivery[field] = delivery[field].isoformat()
            
            key_name = f"staging:delivery:{delivery.get('delivery_id')}"
            self.redis_client.setex(key_name, 1800, json.dumps(delivery))
        
        self.logger.info(f"Staged {len(deliveries)} deliveries in Redis")
        
        # Extract incidents
        self.logger.info("   Extracting incidents...")
        incidents = list(self.db.incidents.find(
            {'timestamp': {'$gte': cutoff_time}},
            {'_id': 0}
        ))
        
        for incident in incidents:
            if 'timestamp' in incident and isinstance(incident['timestamp'], datetime):
                incident['timestamp'] = incident['timestamp'].isoformat()
            
            key_name = f"staging:incident:{incident.get('incident_id')}"
            self.redis_client.setex(key_name, 1800, json.dumps(incident))
        
        self.logger.info(f"Staged {len(incidents)} incidents in Redis")
    
    def validate_staged_data(self) -> Dict[str, int]:
        """
        Validate data quality in Redis staging area
        PURPOSE: Data quality check before loading to Spark
        
        Returns:
            Dictionary with counts per data type
        """
        self.logger.info("STEP 3: VALIDATE staged data quality")
        
        stats = {
            'vehicles': 0,
            'drivers': 0,
            'warehouses': 0,
            'customers': 0,
            'telemetry': 0,
            'deliveries': 0,
            'incidents': 0
        }
        
        # Count staged records by pattern
        for key_type in stats.keys():
            pattern = f"staging:{key_type}:*"
            keys = self.redis_client.keys(pattern)
            stats[key_type] = len(keys)
            self.logger.info(f"   {key_type}: {len(keys)} records staged")
        
        # Validate data integrity
        total_staged = sum(stats.values())
        if total_staged == 0:
            self.logger.warning("No data in staging area!")
        else:
            self.logger.info(f"Total staged records: {total_staged:,}")
        
        return stats
    
    def cache_kpis(self, kpis: Dict[str, Any]):
        """
        Cache calculated KPIs in Redis
        PURPOSE: Cache layer for performance optimization
        
        Args:
            kpis: Dictionary of KPI names and values
        """
        self.logger.info("STEP 4: CACHE KPIs in Redis")
        
        for kpi_name, value in kpis.items():
            key_name = f"cache:kpi:{kpi_name}"
            self.redis_client.setex(
                key_name,
                300,  # 5 minutes TTL for cache
                json.dumps(value)
            )
            self.logger.info(f"   Cached KPI: {kpi_name}")
        
        self.logger.info("KPIs cached for fast retrieval")
    
    def get_staged_data(self, data_type: str) -> List[Dict]:
        """
        Retrieve staged data from Redis
        PURPOSE: Load cleaned data for Spark processing
        
        Args:
            data_type: Type of data (vehicles, drivers, etc.)
            
        Returns:
            List of dictionaries with staged data
        """
        pattern = f"staging:{data_type}:*"
        keys = self.redis_client.keys(pattern)
        
        data = []
        for key in keys:
            value = self.redis_client.get(key)
            if value:
                data.append(json.loads(value))
        
        return data
    
    def clear_staging_area(self):
        """Clear all staged data from Redis"""
        self.logger.info("Clearing staging area...")
        
        patterns = ['staging:*', 'cache:*']
        total_deleted = 0
        
        for pattern in patterns:
            keys = self.redis_client.keys(pattern)
            if keys:
                deleted = self.redis_client.delete(*keys)
                total_deleted += deleted
        
        self.logger.info(f"Cleared {total_deleted} keys from staging/cache")
    
    def get_staging_stats(self) -> Dict[str, Any]:
        """Get statistics about Redis staging area"""
        info = self.redis_client.info('memory')
        
        stats = {
            'used_memory_mb': round(info['used_memory'] / 1024 / 1024, 2),
            'used_memory_peak_mb': round(info['used_memory_peak'] / 1024 / 1024, 2),
            'staging_keys': len(self.redis_client.keys('staging:*')),
            'cache_keys': len(self.redis_client.keys('cache:*')),
            'total_keys': self.redis_client.dbsize()
        }
        
        return stats
    
    def close(self):
        """Close connections"""
        self.mongo_client.close()
        self.logger.info("Connections closed")


def main():
    """Main function for testing"""
    print("=" * 80)
    print("Redis ETL Staging Area")
    print("=" * 80)
    
    staging = RedisETLStaging()
    
    try:
        # Extract and stage dimensions
        staging.extract_and_stage_dimensions()
        
        print()
        
        # Extract and stage recent facts
        staging.extract_and_stage_facts(hours_back=1)
        
        print()
        
        # Validate staged data
        stats = staging.validate_staged_data()
        
        print()
        
        # Show staging statistics
        redis_stats = staging.get_staging_stats()
        print("\n" + "=" * 80)
        print("Redis Staging Statistics")
        print("=" * 80)
        print(f"Memory used: {redis_stats['used_memory_mb']} MB")
        print(f"Staging keys: {redis_stats['staging_keys']:,}")
        print(f"Cache keys: {redis_stats['cache_keys']:,}")
        print(f"Total keys: {redis_stats['total_keys']:,}")
        print("=" * 80)
        
        print("\nETL Staging complete!")
        print("Note: Redis serves DUAL PURPOSE:")
        print("   1. ETL Staging Area - Clean and prepare data")
        print("   2. Cache Layer - Fast KPI retrieval")
        
    finally:
        staging.close()


if __name__ == "__main__":
    main()
