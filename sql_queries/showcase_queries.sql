-- ================================================================================
-- SHOWCASE SQL QUERIES FOR FLEET ANALYTICS
-- These queries demonstrate complex SQL operations: JOINs, WHERE, GROUP BY, HAVING
-- ================================================================================

-- QUERY 1: Driver Performance with Vehicle Analysis
-- Business Question: Which drivers have the best performance metrics by vehicle type?
-- Uses: INNER JOIN (2 tables), WHERE, GROUP BY, HAVING
-- ================================================================================

SELECT 
    d.driver_id,
    d.name AS driver_name,
    d.rating AS driver_rating,
    d.experience_years,
    v.vehicle_type,
    COUNT(t.vehicle_id) AS total_trips,
    ROUND(AVG(t.speed), 2) AS avg_speed,
    ROUND(AVG(t.fuel_level), 2) AS avg_fuel_remaining,
    ROUND(SUM(t.speed * 0.001), 2) AS total_distance_km
FROM 
    dim_drivers d
INNER JOIN 
    telemetry_events t ON d.driver_id = t.driver_id
INNER JOIN 
    dim_vehicles v ON t.vehicle_id = v.vehicle_id
WHERE 
    t.timestamp >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
    AND t.speed > 0
    AND d.status = 'OnRoute'
GROUP BY 
    d.driver_id, d.name, d.rating, d.experience_years, v.vehicle_type
HAVING 
    COUNT(t.vehicle_id) > 100
    AND AVG(t.speed) > 30
ORDER BY 
    driver_rating DESC, total_distance_km DESC
LIMIT 20;

-- ================================================================================
-- QUERY 2: Route Efficiency Analysis with Geographic Data
-- Business Question: What are the most efficient delivery routes by warehouse and customer location?
-- Uses: MULTIPLE JOINs (4 tables), WHERE, GROUP BY, HAVING
-- ================================================================================

SELECT 
    w.warehouse_id,
    w.name AS warehouse_name,
    w.city AS warehouse_city,
    c.customer_type,
    COUNT(DISTINCT del.delivery_id) AS total_deliveries,
    ROUND(AVG(del.distance_km), 2) AS avg_distance_km,
    ROUND(AVG(
        TIMESTAMPDIFF(MINUTE, del.pickup_time, del.estimated_delivery)
    ), 2) AS avg_delivery_time_minutes,
    ROUND(AVG(del.package_weight), 2) AS avg_package_weight,
    ROUND(
        AVG(del.distance_km) / 
        AVG(TIMESTAMPDIFF(MINUTE, del.pickup_time, del.estimated_delivery)) * 60,
        2
    ) AS avg_speed_kmh
FROM 
    dim_warehouses w
INNER JOIN 
    deliveries del ON w.warehouse_id = del.warehouse_id
INNER JOIN 
    dim_customers c ON del.customer_id = c.customer_id
INNER JOIN 
    dim_vehicles v ON del.vehicle_id = v.vehicle_id
WHERE 
    del.status IN ('InProgress', 'Completed')
    AND del.pickup_time >= CURRENT_DATE - INTERVAL 7 DAYS
    AND del.distance_km > 0
GROUP BY 
    w.warehouse_id, w.name, w.city, c.customer_type
HAVING 
    COUNT(DISTINCT del.delivery_id) >= 5
    AND AVG(del.distance_km) > 10
ORDER BY 
    avg_speed_kmh DESC, total_deliveries DESC;

-- ================================================================================
-- QUERY 3: Incident Analysis by Driver Experience and Vehicle Type
-- Business Question: How do incident rates vary by driver experience and vehicle type?
-- Uses: LEFT JOIN (3 tables), WHERE, GROUP BY, HAVING, Complex calculations
-- ================================================================================

SELECT 
    v.vehicle_type,
    CASE 
        WHEN d.experience_years < 5 THEN 'Novice (0-5 years)'
        WHEN d.experience_years < 10 THEN 'Intermediate (5-10 years)'
        WHEN d.experience_years < 15 THEN 'Experienced (10-15 years)'
        ELSE 'Expert (15+ years)'
    END AS experience_level,
    COUNT(DISTINCT i.incident_id) AS total_incidents,
    COUNT(DISTINCT i.driver_id) AS unique_drivers,
    ROUND(
        COUNT(DISTINCT i.incident_id) * 1.0 / COUNT(DISTINCT i.driver_id),
        2
    ) AS incidents_per_driver,
    SUM(CASE WHEN i.incident_type = 'harsh_braking' THEN 1 ELSE 0 END) AS harsh_braking_count,
    SUM(CASE WHEN i.incident_type = 'speeding' THEN 1 ELSE 0 END) AS speeding_count,
    SUM(CASE WHEN i.incident_type = 'harsh_acceleration' THEN 1 ELSE 0 END) AS harsh_accel_count,
    SUM(CASE WHEN i.severity = 'High' THEN 1 ELSE 0 END) AS high_severity_count,
    ROUND(
        SUM(CASE WHEN i.severity = 'High' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) AS high_severity_percentage
FROM 
    dim_vehicles v
INNER JOIN 
    incidents i ON v.vehicle_id = i.vehicle_id
LEFT JOIN 
    dim_drivers d ON i.driver_id = d.driver_id
WHERE 
    i.timestamp >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY 
    v.vehicle_type, experience_level
HAVING 
    COUNT(DISTINCT i.incident_id) >= 3
ORDER BY 
    incidents_per_driver DESC, high_severity_percentage DESC;

-- ================================================================================
-- QUERY 4: Delivery Time Variance by Location and Vehicle Capacity
-- Business Question: How does delivery time vary by customer location and vehicle capacity?
-- Uses: INNER JOIN (4 tables), WHERE, GROUP BY, HAVING, Statistical functions
-- ================================================================================

SELECT 
    c.city,
    c.customer_type,
    CASE 
        WHEN v.capacity_kg < 1000 THEN 'Small (<1000kg)'
        WHEN v.capacity_kg < 2000 THEN 'Medium (1000-2000kg)'
        WHEN v.capacity_kg < 3000 THEN 'Large (2000-3000kg)'
        ELSE 'Extra Large (3000kg+)'
    END AS vehicle_capacity_class,
    COUNT(del.delivery_id) AS total_deliveries,
    ROUND(AVG(
        TIMESTAMPDIFF(MINUTE, del.pickup_time, del.estimated_delivery)
    ), 2) AS avg_delivery_time_min,
    ROUND(STDDEV(
        TIMESTAMPDIFF(MINUTE, del.pickup_time, del.estimated_delivery)
    ), 2) AS delivery_time_variance,
    MIN(TIMESTAMPDIFF(MINUTE, del.pickup_time, del.estimated_delivery)) AS min_delivery_time,
    MAX(TIMESTAMPDIFF(MINUTE, del.pickup_time, del.estimated_delivery)) AS max_delivery_time,
    ROUND(AVG(del.package_weight), 2) AS avg_package_weight,
    ROUND(AVG(del.distance_km), 2) AS avg_distance
FROM 
    deliveries del
INNER JOIN 
    dim_customers c ON del.customer_id = c.customer_id
INNER JOIN 
    dim_vehicles v ON del.vehicle_id = v.vehicle_id
INNER JOIN 
    dim_drivers d ON del.driver_id = d.driver_id
WHERE 
    del.status = 'Completed'
    AND del.pickup_time >= CURRENT_DATE - INTERVAL 14 DAYS
    AND del.distance_km > 0
GROUP BY 
    c.city, c.customer_type, vehicle_capacity_class
HAVING 
    COUNT(del.delivery_id) >= 10
    AND AVG(TIMESTAMPDIFF(MINUTE, del.pickup_time, del.estimated_delivery)) > 0
ORDER BY 
    delivery_time_variance DESC, avg_delivery_time_min ASC;

-- ================================================================================
-- QUERY 5: Fuel Consumption Analysis with Multi-Dimensional Aggregation
-- Business Question: What is the fuel consumption pattern across different dimensions?
-- Uses: MULTIPLE JOINs (4 tables), WHERE, GROUP BY, HAVING, Window functions
-- ================================================================================

SELECT 
    v.make,
    v.vehicle_type,
    v.year,
    d.experience_years,
    w.city AS origin_city,
    COUNT(DISTINCT t.vehicle_id) AS unique_vehicles,
    COUNT(*) AS total_readings,
    ROUND(AVG(t.fuel_level), 2) AS avg_fuel_level,
    ROUND(AVG(t.speed), 2) AS avg_speed,
    ROUND(AVG(t.engine_temp), 2) AS avg_engine_temp,
    ROUND(
        (MAX(t.fuel_level) - MIN(t.fuel_level)) / 
        NULLIF(COUNT(DISTINCT DATE(t.timestamp)), 0),
        2
    ) AS avg_daily_fuel_consumption,
    ROUND(
        AVG(t.speed) / NULLIF(AVG(t.fuel_level), 0) * 100,
        2
    ) AS fuel_efficiency_index,
    SUM(CASE WHEN t.fuel_level < 20 THEN 1 ELSE 0 END) AS low_fuel_instances
FROM 
    telemetry_events t
INNER JOIN 
    dim_vehicles v ON t.vehicle_id = v.vehicle_id
LEFT JOIN 
    dim_drivers d ON t.driver_id = d.driver_id
LEFT JOIN 
    deliveries del ON t.vehicle_id = del.vehicle_id 
        AND DATE(t.timestamp) = DATE(del.pickup_time)
LEFT JOIN 
    dim_warehouses w ON del.warehouse_id = w.warehouse_id
WHERE 
    t.timestamp >= CURRENT_DATE - INTERVAL 7 DAYS
    AND t.speed > 0
    AND t.fuel_level > 0
GROUP BY 
    v.make, v.vehicle_type, v.year, d.experience_years, w.city
HAVING 
    COUNT(*) >= 100
    AND AVG(t.fuel_level) > 10
    AND COUNT(DISTINCT t.vehicle_id) >= 2
ORDER BY 
    fuel_efficiency_index DESC, avg_daily_fuel_consumption ASC
LIMIT 50;

-- ================================================================================
-- END OF SHOWCASE QUERIES
-- ================================================================================
-- Summary of SQL Features Demonstrated:
-- ✅ INNER JOIN (2-4 tables per query)
-- ✅ LEFT JOIN for optional relationships
-- ✅ WHERE clauses with multiple conditions
-- ✅ GROUP BY with multiple columns
-- ✅ HAVING clauses for aggregate filtering
-- ✅ Aggregate functions (COUNT, AVG, SUM, MIN, MAX, STDDEV)
-- ✅ CASE statements for conditional logic
-- ✅ Date/time functions (TIMESTAMPDIFF, INTERVAL, CURRENT_DATE)
-- ✅ Mathematical calculations
-- ✅ ORDER BY with multiple columns
-- ✅ LIMIT for result pagination
-- ================================================================================
