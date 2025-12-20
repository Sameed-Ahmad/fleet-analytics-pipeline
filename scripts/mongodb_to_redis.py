#!/usr/bin/env python3
"""
MongoDB to Redis Data Cache Script - COMPLETE VERSION
Extracts analytics data from MongoDB and caches in Redis for Grafana
Calculates ALL KPIs for real-time dashboard updates
"""
import redis
import json
from pymongo import MongoClient
from datetime import datetime, timedelta

print("="*70)
print("MongoDB to Redis Data Cache Script")
print("="*70)
print()

print("Connecting to databases...")
# Connect to databases
mongo_client = MongoClient('mongodb://admin:admin123@localhost:27017/')
db = mongo_client['fleet_analytics']
r = redis.Redis(host='localhost', port=6379, password='redis123', decode_responses=True)

print(" Connected to MongoDB and Redis\n")

# ====================
# 1. Top 10 Drivers by Rating
# ====================
print(" Processing: Top 10 Drivers by Rating...")
top_drivers = list(db.dim_drivers.find(
    {}, 
    {'_id': 0, 'name': 1, 'rating': 1, 'experience_years': 1}
).sort('rating', -1).limit(10))

if top_drivers:
    # Store as JSON for display
    r.set('cache:mongo:top_drivers', json.dumps(top_drivers))
    
    # Also store individual drivers for easier access
    for i, driver in enumerate(top_drivers, 1):
        r.set(f'cache:driver:{i}:name', driver['name'])
        r.set(f'cache:driver:{i}:rating', str(driver['rating']))
    
    # Store just the top driver name and rating
    r.set('cache:stat:top_driver_name', top_drivers[0]['name'])
    r.set('cache:stat:top_driver_rating', str(top_drivers[0]['rating']))
    
    print(f"   ✓ Stored {len(top_drivers)} top drivers")
    for i, driver in enumerate(top_drivers[:3], 1):
        print(f"     {i}. {driver['name']}: {driver['rating']}")
else:
    print("   ⚠ No drivers found in database")

# ====================
# 2. Vehicles by Type
# ====================
print("\n Processing: Vehicles by Type...")
vehicles_by_type = list(db.dim_vehicles.aggregate([
    {'$group': {'_id': '$vehicle_type', 'count': {'$sum': 1}}}
]))

if vehicles_by_type:
    # Store as JSON object
    vehicle_data = {item['_id']: item['count'] for item in vehicles_by_type}
    r.set('cache:mongo:vehicles_by_type', json.dumps(vehicle_data))
    
    # Store individual counts for bar chart (IMPORTANT for Grafana)
    for vtype, count in vehicle_data.items():
        r.set(f'cache:stat:vehicles_{vtype.lower()}', str(count))
        r.set(f'cache:mongo:vehicle_type:{vtype}', str(count))
    
    print(f"   ✓ Stored vehicle types: {vehicle_data}")
else:
    print("   ⚠ No vehicles found in database")

# ====================
# 3. Recent Incidents (Last 10) - FORMATTED
# ====================
print("\n Processing: Recent Incidents...")
recent_incidents = list(db.incidents.find(
    {}, 
    {'_id': 0, 'vehicle_id': 1, 'driver_id': 1, 'incident_type': 1, 
     'severity': 1, 'timestamp': 1}
).sort('timestamp', -1).limit(10))

if recent_incidents:
    # Format for clean display
    formatted_incidents = []
    for incident in recent_incidents:
        time_str = incident['timestamp'].strftime('%H:%M:%S') if isinstance(incident['timestamp'], datetime) else str(incident['timestamp'])[:8]
        formatted_incidents.append({
            'time': time_str,
            'vehicle': incident.get('vehicle_id', 'N/A'),
            'type': incident.get('incident_type', 'N/A').replace('_', ' ').title(),
            'severity': incident.get('severity', 'N/A').capitalize()
        })
    
    # Store formatted version
    r.set('cache:table:incidents_formatted', json.dumps(formatted_incidents))
    
    # Also store original with ISO timestamps
    for incident in recent_incidents:
        if 'timestamp' in incident and isinstance(incident['timestamp'], datetime):
            incident['timestamp'] = incident['timestamp'].isoformat()
    r.set('cache:mongo:recent_incidents', json.dumps(recent_incidents))
    
    print(f"   ✓ Stored {len(recent_incidents)} incidents (formatted)")
else:
    print("   ⚠ No incidents found in database")

# ====================
# 4. Average Speed Over Time (Last 24 hours)
# ====================
print("\n Processing: Average Speed Over Time...")
twenty_four_hours_ago = datetime.now() - timedelta(hours=24)

speed_timeseries = list(db.telemetry_events.aggregate([
    {'$match': {'timestamp': {'$gte': twenty_four_hours_ago}}},
    {'$group': {
        '_id': {
            '$dateToString': {
                'format': '%Y-%m-%d %H:%M',
                'date': '$timestamp'
            }
        },
        'avg_speed': {'$avg': '$speed'}
    }},
    {'$sort': {'_id': 1}}
]))

avg_speed = None
if speed_timeseries:
    r.set('cache:mongo:avg_speed_timeseries', json.dumps(speed_timeseries))
    avg_speed = sum(item['avg_speed'] for item in speed_timeseries) / len(speed_timeseries)
    r.set('cache:stat:fleet_avg_speed', str(round(avg_speed, 1)))
    print(f"   ✓ Stored {len(speed_timeseries)} speed data points")
    print(f"   ✓ Average speed: {round(avg_speed, 1)} km/h")
else:
    # Fallback to all data
    all_telemetry = db.telemetry_events.count_documents({})
    if all_telemetry > 0:
        avg_result = list(db.telemetry_events.aggregate([
            {'$group': {'_id': None, 'avg_speed': {'$avg': '$speed'}}}
        ]))
        if avg_result:
            avg_speed = avg_result[0]['avg_speed']
            r.set('cache:stat:fleet_avg_speed', str(round(avg_speed, 1)))
            print(f"   ✓ Using overall average: {round(avg_speed, 1)} km/h")

# ====================
# 5. Fuel Level Distribution (Last 24 hours)
# ====================
print("\n Processing: Fuel Level Distribution...")
fuel_timeseries = list(db.telemetry_events.aggregate([
    {'$match': {'timestamp': {'$gte': twenty_four_hours_ago}}},
    {'$group': {
        '_id': {
            '$dateToString': {
                'format': '%Y-%m-%d %H:%M',
                'date': '$timestamp'
            }
        },
        'avg_fuel': {'$avg': '$fuel_level'}
    }},
    {'$sort': {'_id': 1}}
]))

avg_fuel = None
if fuel_timeseries:
    r.set('cache:mongo:fuel_level_timeseries', json.dumps(fuel_timeseries))
    avg_fuel = sum(item['avg_fuel'] for item in fuel_timeseries) / len(fuel_timeseries)
    r.set('cache:stat:fleet_avg_fuel', str(round(avg_fuel, 1)))
    print(f"   ✓ Stored {len(fuel_timeseries)} fuel data points")
    print(f"   ✓ Average fuel: {round(avg_fuel, 1)}%")
else:
    # Fallback to all data
    all_telemetry = db.telemetry_events.count_documents({})
    if all_telemetry > 0:
        avg_result = list(db.telemetry_events.aggregate([
            {'$group': {'_id': None, 'avg_fuel': {'$avg': '$fuel_level'}}}
        ]))
        if avg_result:
            avg_fuel = avg_result[0]['avg_fuel']
            r.set('cache:stat:fleet_avg_fuel', str(round(avg_fuel, 1)))
            print(f"   ✓ Using overall average: {round(avg_fuel, 1)}%")

# ====================
# 6. Calculate ALL Real-Time KPIs
# ====================
print("\n Processing: Real-Time KPIs...")

# Get counts
total_telemetry = db.telemetry_events.count_documents({})
total_deliveries = db.deliveries.count_documents({})
total_incidents = db.incidents.count_documents({})
total_drivers = db.dim_drivers.count_documents({})
total_vehicles = db.dim_vehicles.count_documents({})

# Active Vehicles (vehicles with telemetry in last 5 minutes)
five_minutes_ago = datetime.now() - timedelta(minutes=5)
active_vehicles_list = db.telemetry_events.distinct('vehicle_id', 
    {'timestamp': {'$gte': five_minutes_ago}})
active_vehicles_count = len(active_vehicles_list)

# Drivers On Route (drivers with active deliveries)
drivers_on_route_list = db.deliveries.distinct('driver_id', 
    {'status': {'$in': ['InProgress', 'Pending']}})
drivers_on_route_count = len(drivers_on_route_list)

# Fleet Utilization (active vehicles / total vehicles * 100)
fleet_utilization = (active_vehicles_count / total_vehicles * 100) if total_vehicles > 0 else 0

# Delivery Success Rate
completed_deliveries = db.deliveries.count_documents({'status': 'Completed'})
delivery_success_rate = (completed_deliveries / total_deliveries * 100) if total_deliveries > 0 else 0

# Average Driver Rating
avg_rating_result = list(db.dim_drivers.aggregate([
    {'$group': {'_id': None, 'avg_rating': {'$avg': '$rating'}}}
]))
avg_driver_rating = avg_rating_result[0]['avg_rating'] if avg_rating_result else 0

# Store ALL KPIs
r.set('cache:kpi:total_vehicles', str(total_vehicles))
r.set('cache:kpi:active_vehicles', str(active_vehicles_count))
r.set('cache:kpi:fleet_utilization_percent', str(round(fleet_utilization, 1)))
r.set('cache:kpi:drivers_on_route', str(drivers_on_route_count))
r.set('cache:kpi:avg_speed_kmh', str(round(avg_speed, 1)) if avg_speed else '0')
r.set('cache:kpi:total_incidents', str(total_incidents))
r.set('cache:kpi:delivery_success_rate_percent', str(round(delivery_success_rate, 1)))
r.set('cache:kpi:total_deliveries', str(total_deliveries))
r.set('cache:kpi:avg_driver_rating', str(round(avg_driver_rating, 2)))
r.set('cache:kpi:avg_fuel_level_percent', str(round(avg_fuel, 1)) if avg_fuel else '0')

# Also store as stat keys for compatibility
r.set('cache:stat:total_telemetry', str(total_telemetry))
r.set('cache:stat:total_deliveries', str(total_deliveries))
r.set('cache:stat:total_incidents', str(total_incidents))
r.set('cache:stat:total_drivers', str(total_drivers))
r.set('cache:stat:total_vehicles', str(total_vehicles))

print(f"   ✓ Active vehicles: {active_vehicles_count}/{total_vehicles}")
print(f"   ✓ Fleet utilization: {round(fleet_utilization, 1)}%")
print(f"   ✓ Drivers on route: {drivers_on_route_count}")
print(f"   ✓ Total deliveries: {total_deliveries}")
print(f"   ✓ Delivery success rate: {round(delivery_success_rate, 1)}%")
print(f"   ✓ Average driver rating: {round(avg_driver_rating, 2)}")
print(f"   ✓ All KPIs cached successfully")

# ====================
# Summary Report
# ====================
print("\n" + "="*70)
print(" SUMMARY STATISTICS")
print("="*70)
print(f"Total Vehicles:          {total_vehicles}")
print(f"Active Vehicles:         {active_vehicles_count} ({round(fleet_utilization, 1)}%)")
print(f"Total Drivers:           {total_drivers}")
print(f"Drivers On Route:        {drivers_on_route_count}")
print(f"Total Telemetry Events:  {total_telemetry:,}")
print(f"Total Deliveries:        {total_deliveries}")
print(f"Delivery Success Rate:   {round(delivery_success_rate, 1)}%")
print(f"Total Incidents:         {total_incidents}")
print(f"Average Speed:           {round(avg_speed, 1) if avg_speed else 0} km/h")
print(f"Average Fuel:            {round(avg_fuel, 1) if avg_fuel else 0}%")
print(f"Average Driver Rating:   {round(avg_driver_rating, 2)}")

# Calculate data size
try:
    stats = db.command('dbStats')
    data_size_mb = stats['dataSize'] / (1024 * 1024)
    print(f"Database Size:           {data_size_mb:.2f} MB")
except:
    pass

print("\n" + "="*70)
print(" ALL DATA CACHED TO REDIS!")
print("="*70)
print("\n All Grafana KPIs are now live and updating!")
print()