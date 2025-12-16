#!/usr/bin/env python3
"""
Spark SQL Analytics Engine - Fixed for type conflicts
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pymongo import MongoClient
import redis
import json
from datetime import datetime

class SparkSQLAnalyticsSimple:
    """Simplified Spark SQL Analytics Engine"""
    
    def __init__(self, app_name="FleetAnalytics"):
        self.logger = self._setup_logger()
        self.logger.info(" Initializing Spark SQL Analytics Engine...")
        
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.mongo_client = MongoClient('mongodb://admin:admin123@localhost:27017/')
        self.db = self.mongo_client['fleet_analytics']
        
        self.redis_client = redis.Redis(
            host='localhost',
            port=6379,
            password='redis123',
            decode_responses=True
        )
        
        self.logger.info(" Spark session created")
    
    def _setup_logger(self):
        logger = logging.getLogger('SparkSQLAnalytics')
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger
    
    def load_data_from_mongodb(self):
        """Load data from MongoDB with proper schemas"""
        self.logger.info(" Loading data from MongoDB...")
        
        try:
            # Load vehicles
            self.logger.info("   Loading vehicles...")
            vehicles_list = list(self.db.dim_vehicles.find({}, {'_id': 0}))
            self.df_vehicles = self.spark.createDataFrame(vehicles_list)
            self.df_vehicles.createOrReplaceTempView("vehicles")
            self.logger.info(f"    Loaded {self.df_vehicles.count()} vehicles")
            
            # Load drivers
            self.logger.info("   Loading drivers...")
            drivers_list = list(self.db.dim_drivers.find({}, {'_id': 0}))
            self.df_drivers = self.spark.createDataFrame(drivers_list)
            self.df_drivers.createOrReplaceTempView("drivers")
            self.logger.info(f"    Loaded {self.df_drivers.count()} drivers")
            
            # Load warehouses
            self.logger.info("   Loading warehouses...")
            warehouses_list = list(self.db.dim_warehouses.find({}, {'_id': 0}))
            for w in warehouses_list:
                if 'location' in w and isinstance(w['location'], dict):
                    w['location_lat'] = float(w['location'].get('latitude', 0))
                    w['location_lon'] = float(w['location'].get('longitude', 0))
                    del w['location']
            self.df_warehouses = self.spark.createDataFrame(warehouses_list)
            self.df_warehouses.createOrReplaceTempView("warehouses")
            self.logger.info(f"    Loaded {self.df_warehouses.count()} warehouses")
            
            # Load customers
            self.logger.info("   Loading customers...")
            customers_list = list(self.db.dim_customers.find({}, {'_id': 0}).limit(1000))
            for c in customers_list:
                if 'location' in c and isinstance(c, dict):
                    c['location_lat'] = float(c['location'].get('latitude', 0))
                    c['location_lon'] = float(c['location'].get('longitude', 0))
                    del c['location']
            self.df_customers = self.spark.createDataFrame(customers_list)
            self.df_customers.createOrReplaceTempView("customers")
            self.logger.info(f"    Loaded {self.df_customers.count()} customers")
            
            # Load telemetry with explicit type conversion
            self.logger.info("   Loading telemetry...")
            telemetry_list = list(self.db.telemetry_events.find({}, {'_id': 0}).limit(5000))
            
            if telemetry_list:
                # Convert all numeric fields to float
                for event in telemetry_list:
                    for field in ['speed', 'fuel_level', 'engine_temp', 'odometer', 'latitude', 'longitude']:
                        if field in event and event[field] is not None:
                            event[field] = float(event[field])
                
                self.df_telemetry = self.spark.createDataFrame(telemetry_list)
                self.df_telemetry.createOrReplaceTempView("telemetry")
                self.logger.info(f"    Loaded {self.df_telemetry.count()} telemetry events")
            else:
                self.logger.warning("     No telemetry data available")
            
            # Load deliveries
            self.logger.info("   Loading deliveries...")
            deliveries_list = list(self.db.deliveries.find({}, {'_id': 0}).limit(1000))
            
            for d in deliveries_list:
                # Convert numeric fields
                if 'distance_km' in d:
                    d['distance_km'] = float(d.get('distance_km', 0))
                if 'package_weight' in d:
                    d['package_weight'] = float(d.get('package_weight', 0))
                
                # Flatten location fields
                if 'pickup_location' in d and isinstance(d['pickup_location'], dict):
                    d['pickup_lat'] = float(d['pickup_location'].get('latitude', 0))
                    d['pickup_lon'] = float(d['pickup_location'].get('longitude', 0))
                    del d['pickup_location']
                if 'delivery_location' in d and isinstance(d['delivery_location'], dict):
                    d['delivery_lat'] = float(d['delivery_location'].get('latitude', 0))
                    d['delivery_lon'] = float(d['delivery_location'].get('longitude', 0))
                    del d['delivery_location']
            
            if deliveries_list:
                self.df_deliveries = self.spark.createDataFrame(deliveries_list)
                self.df_deliveries.createOrReplaceTempView("deliveries")
                self.logger.info(f"    Loaded {self.df_deliveries.count()} deliveries")
            else:
                self.logger.warning("     No deliveries available")
            
            # Load incidents
            self.logger.info("   Loading incidents...")
            incidents_list = list(self.db.incidents.find({}, {'_id': 0}).limit(1000))
            
            for i in incidents_list:
                if 'speed_at_incident' in i:
                    i['speed_at_incident'] = float(i.get('speed_at_incident', 0))
                if 'latitude' in i:
                    i['latitude'] = float(i.get('latitude', 0))
                if 'longitude' in i:
                    i['longitude'] = float(i.get('longitude', 0))
            
            if incidents_list:
                self.df_incidents = self.spark.createDataFrame(incidents_list)
                self.df_incidents.createOrReplaceTempView("incidents")
                self.logger.info(f"   Loaded {self.df_incidents.count()} incidents")
            else:
                self.logger.warning("    No incidents available")
            
            self.logger.info(" All data loaded from MongoDB into Spark")
            
        except Exception as e:
            self.logger.error(f" Error loading data: {e}")
            raise
    
    def run_showcase_query_1(self):
        """Query 1: Driver Performance with Vehicle Analysis"""
        self.logger.info("\n Query 1: Driver Performance Analysis")
        
        try:
            query = """
            SELECT 
                d.driver_id,
                d.name AS driver_name,
                d.rating AS driver_rating,
                d.experience_years,
                v.vehicle_type,
                COUNT(t.vehicle_id) AS total_trips,
                ROUND(AVG(t.speed), 2) AS avg_speed,
                ROUND(AVG(t.fuel_level), 2) AS avg_fuel_remaining
            FROM 
                drivers d
            INNER JOIN 
                telemetry t ON d.driver_id = t.driver_id
            INNER JOIN 
                vehicles v ON t.vehicle_id = v.vehicle_id
            WHERE 
                t.speed > 0
            GROUP BY 
                d.driver_id, d.name, d.rating, d.experience_years, v.vehicle_type
            HAVING 
                COUNT(t.vehicle_id) > 5
                AND AVG(t.speed) > 20
            ORDER BY 
                driver_rating DESC
            LIMIT 20
            """
            
            result = self.spark.sql(query)
            count = result.count()
            
            self.logger.info(f"   Found {count} drivers")
            if count > 0:
                result.show(10, truncate=False)
            
            return result
        except Exception as e:
            self.logger.error(f"   Error: {e}")
            return None
    
    def run_showcase_query_2(self):
        """Query 2: Route Efficiency"""
        self.logger.info("\n Query 2: Route Efficiency Analysis")
        
        try:
            query = """
            SELECT 
                w.warehouse_id,
                w.name AS warehouse_name,
                w.city,
                c.customer_type,
                COUNT(DISTINCT del.delivery_id) AS total_deliveries,
                ROUND(AVG(del.distance_km), 2) AS avg_distance_km
            FROM 
                warehouses w
            INNER JOIN 
                deliveries del ON w.warehouse_id = del.warehouse_id
            INNER JOIN 
                customers c ON del.customer_id = c.customer_id
            INNER JOIN 
                vehicles v ON del.vehicle_id = v.vehicle_id
            WHERE 
                del.distance_km > 0
            GROUP BY 
                w.warehouse_id, w.name, w.city, c.customer_type
            HAVING 
                COUNT(DISTINCT del.delivery_id) >= 1
            ORDER BY 
                total_deliveries DESC
            """
            
            result = self.spark.sql(query)
            count = result.count()
            
            self.logger.info(f"   Found {count} routes")
            if count > 0:
                result.show(10, truncate=False)
            
            return result
        except Exception as e:
            self.logger.error(f"   Error: {e}")
            return None
    
    def run_showcase_query_3(self):
        """Query 3: Incident Analysis"""
        self.logger.info("\n Query 3: Incident Analysis")
        
        try:
            query = """
            SELECT 
                v.vehicle_type,
                CASE 
                    WHEN d.experience_years < 5 THEN 'Novice'
                    WHEN d.experience_years < 10 THEN 'Intermediate'
                    ELSE 'Expert'
                END AS experience_level,
                COUNT(DISTINCT i.incident_id) AS total_incidents
            FROM 
                vehicles v
            INNER JOIN 
                incidents i ON v.vehicle_id = i.vehicle_id
            LEFT JOIN 
                drivers d ON i.driver_id = d.driver_id
            GROUP BY 
                v.vehicle_type, experience_level
            ORDER BY 
                total_incidents DESC
            """
            
            result = self.spark.sql(query)
            count = result.count()
            
            self.logger.info(f"   Found {count} patterns")
            if count > 0:
                result.show(truncate=False)
            
            return result
        except Exception as e:
            self.logger.error(f"   Error: {e}")
            return None
    
    def calculate_kpis(self):
        """Calculate KPIs"""
        self.logger.info("\n Calculating KPIs...")
        
        kpis = {}
        
        try:
            kpis['total_vehicles'] = self.spark.sql("SELECT COUNT(*) as c FROM vehicles").first()['c']
            kpis['total_drivers'] = self.spark.sql("SELECT COUNT(*) as c FROM drivers").first()['c']
            kpis['avg_driver_rating'] = round(self.spark.sql("SELECT AVG(rating) as a FROM drivers").first()['a'], 2)
            kpis['total_warehouses'] = self.spark.sql("SELECT COUNT(*) as c FROM warehouses").first()['c']
            kpis['total_customers'] = self.spark.sql("SELECT COUNT(*) as c FROM customers").first()['c']
            
            try:
                kpis['avg_speed'] = round(self.spark.sql("SELECT AVG(speed) as a FROM telemetry WHERE speed > 0").first()['a'], 2)
                kpis['avg_fuel'] = round(self.spark.sql("SELECT AVG(fuel_level) as a FROM telemetry").first()['a'], 2)
            except:
                kpis['avg_speed'] = 0
                kpis['avg_fuel'] = 0
            
            try:
                kpis['total_deliveries'] = self.spark.sql("SELECT COUNT(*) as c FROM deliveries").first()['c']
            except:
                kpis['total_deliveries'] = 0
            
            try:
                kpis['total_incidents'] = self.spark.sql("SELECT COUNT(*) as c FROM incidents").first()['c']
            except:
                kpis['total_incidents'] = 0
            
            self.logger.info(f" Calculated {len(kpis)} KPIs")
            
        except Exception as e:
            self.logger.error(f"Error: {e}")
        
        return kpis
    
    def print_kpis(self, kpis):
        print("\n" + "=" * 80)
        print(" FLEET ANALYTICS KPIs")
        print("=" * 80)
        for k, v in kpis.items():
            print(f"{k.replace('_', ' ').title():.<50} {v}")
        print("=" * 80)
    
    def cache_kpis(self, kpis):
        """Cache KPIs in Redis"""
        self.logger.info("\n Caching KPIs in Redis...")
        for k, v in kpis.items():
            self.redis_client.setex(
                f"cache:kpi:{k}",
                300,
                json.dumps({'value': v, 'timestamp': datetime.utcnow().isoformat()})
            )
        self.logger.info(f" Cached {len(kpis)} KPIs")
    
    def close(self):
        self.logger.info("\n Closing Spark session...")
        self.spark.stop()
        self.mongo_client.close()
        self.logger.info(" Connections closed")


def main():
    print("=" * 80)
    print("Spark SQL Analytics Engine - Fleet Analytics")
    print("=" * 80)
    
    analytics = SparkSQLAnalyticsSimple()
    
    try:
        analytics.load_data_from_mongodb()
        analytics.run_showcase_query_1()
        analytics.run_showcase_query_2()
        analytics.run_showcase_query_3()
        
        kpis = analytics.calculate_kpis()
        analytics.print_kpis(kpis)
        analytics.cache_kpis(kpis)
        
        print("\n Analytics complete!")
        
    except Exception as e:
        print(f"\n Error: {e}")
    finally:
        analytics.close()


if __name__ == "__main__":
    main()