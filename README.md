# Fleet Analytics Pipeline - Real-Time Data Processing System

[![Python](https://img.shields.io/badge/Python-3.11%2B-blue)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Required-blue)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

## 📋 Project Overview

A comprehensive **Big Data Analytics pipeline** for real-time fleet management, processing vehicle telemetry, deliveries, and incidents. The system demonstrates modern data engineering practices with streaming data processing, real-time analytics, and automated orchestration.

### 🎯 Key Features

- **Real-time data generation** using statistical models (Markov Chain, Gaussian, Poisson, AR(1), HMM)
- **Streaming data processing** via Apache Kafka
- **Multi-database architecture** (MongoDB for operational data, Redis for caching)
- **3-tier data archiving** (HOT → WARM → COLD) with HDFS
- **Real-time dashboard** with Grafana (60-second auto-refresh)
- **Workflow orchestration** with Apache Airflow (7 DAGs)
- **Docker containerization** for single-command deployment

---

## 🏗️ Architecture
```
Data Generators → Kafka → Spark Streaming → MongoDB → Redis → Grafana
     ↓                        ↓                ↓         ↓
Statistical Models      Real-time ETL    3-Tier      Dashboard
(5 models)              Processing       Archiving   (10 KPIs)
```

### Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Generation** | Python, Statistical Models | Generate realistic fleet data |
| **Message Queue** | Apache Kafka | Stream vehicle telemetry |
| **Stream Processing** | Apache Spark | Real-time data transformations |
| **Operational DB** | MongoDB | Store current fleet data |
| **Caching Layer** | Redis | KPI caching & ETL staging |
| **Data Warehouse** | HDFS | Long-term data archiving |
| **Orchestration** | Apache Airflow | Workflow automation |
| **Visualization** | Grafana | Real-time dashboard |
| **Containerization** | Docker | Service deployment |

---

## 📊 Data Pipeline

### 1. Statistical Data Generation

Fleet data is generated using sophisticated statistical models:

- **Markov Chain**: Vehicle route generation with realistic GPS coordinates
- **Gaussian Distribution**: Vehicle speed based on road type and conditions  
- **Poisson Distribution**: Incident generation (harsh braking, speeding)
- **AR(1) Model**: Engine temperature auto-regression
- **Hidden Markov Model (HMM)**: Driver behavior patterns (normal/aggressive/tired)

### 2. Data Flow
```
Generator (Python) 
    ↓ (Kafka Producer)
Kafka Topics (vehicle-telemetry, deliveries, incidents)
    ↓ (Kafka Consumer)
MongoDB (fleet_analytics database)
    ↓ (MongoDB Aggregations)
Redis (KPI Caching + ETL Staging)
    ↓ (Redis Data Source)
Grafana Dashboard (Auto-refresh: 60s)
```

### 3. Three-Tier Data Archiving

| Tier | Storage | Retention | Format | Purpose |
|------|---------|-----------|--------|---------|
| **HOT** | MongoDB | 0-48 hours | JSON | Real-time queries |
| **WARM** | HDFS (/warehouse/warm) | 2-30 days | Parquet | Recent analytics |
| **COLD** | HDFS (/warehouse/cold) | 30+ days | ORC (compressed) | Historical archive |

---

## 💾 Database Schema

### Collections (8 Total)

1. **telemetry_events** - Vehicle sensor data (speed, fuel, location)
2. **deliveries** - Delivery status and tracking
3. **incidents** - Safety events (harsh braking, speeding)
4. **dim_vehicles** - Vehicle master data (500 vehicles)
5. **dim_drivers** - Driver information (350 drivers)
6. **dim_warehouses** - Warehouse locations (20 warehouses)
7. **dim_customers** - Customer database (10,000 customers)
8. **telemetry_aggregations** - Pre-computed analytics

### Data Volume

- **Total Records**: 50,000+ telemetry events
- **Data Size**: 300+ MB (exceeds requirement)
- **Generation Rate**: ~100 events/second during active generation

---

## 📈 Dashboard & KPIs

### Grafana Dashboard (10 Panels)

| KPI | Description | Update Frequency |
|-----|-------------|------------------|
| Total Vehicles | Fleet size | Static (500) |
| Average Driver Rating | Driver performance | 60 seconds |
| Drivers On Route | Active drivers | 60 seconds |
| Average Speed | Fleet-wide speed | 60 seconds |
| Total Incidents | Safety events | 60 seconds |
| Average Speed Over Time | Speed trends | 60 seconds |
| Fuel Level Distribution | Fuel analytics | 60 seconds |
| Delivery Success Rate | Delivery KPI | 60 seconds |
| Total Deliveries | Completed deliveries | 60 seconds |
| Vehicles by Type | Fleet composition | Static |

**Access**: http://localhost:3000 (admin/admin123)

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- 16GB RAM minimum
- 20GB free disk space

### Installation
```bash
# 1. Clone repository
git clone https://github.com/Sameed-Ahmad/fleet-analytics-pipeline.git
cd fleet-analytics-pipeline

# 2. Create Python virtual environment
python3 -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Start all Docker services
docker-compose up -d

# Wait 2-3 minutes for all services to initialize
```

### Run the Pipeline
```bash
# Start the complete data pipeline
./scripts/start_pipeline.sh

# The script will start:
# - MongoDB Kafka Consumer (writes to database)
# - Fleet Data Generator (creates events every 3 seconds)
# - Redis Cache Updater (refreshes KPIs every 60 seconds)

# Press Ctrl+C to stop all components
```

### Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana Dashboard | http://localhost:3000 | admin / admin123 |
| Airflow UI | http://localhost:8080 | admin / admin123 |
| Spark Master UI | http://localhost:8081 | No auth |
| HDFS NameNode | http://localhost:9870 | No auth |

---

## 📁 Project Structure
```
fleet-analytics-pipeline/
├── data_generators/          # Statistical data generators
│   ├── fleet_generator_kafka.py
│   ├── statistical_models/   # Markov, Gaussian, Poisson, AR(1), HMM
│   └── utils/
├── kafka_services/
│   ├── producers/            # Kafka producers
│   └── consumers/            # MongoDB consumer
├── scripts/
│   ├── start_pipeline.sh     # Main startup script
│   ├── stop_pipeline.sh      # Graceful shutdown
│   └── mongodb_to_redis.py   # KPI calculator
├── grafana/
│   ├── dashboards/           # Dashboard JSON
│   └── provisioning/         # Data source configs
├── airflow/
│   └── dags/                 # 7 Airflow DAGs
├── docker-compose.yml        # All services definition
├── requirements.txt          # Python dependencies
└── README.md
```

---

## 🔄 Airflow DAGs (7 Total)

| DAG | Purpose | Schedule |
|-----|---------|----------|
| `data_generation_dag` | Trigger data generators | Continuous |
| `stream_processing_dag` | Monitor Spark jobs | Continuous |
| `etl_staging_dag` | MongoDB → Redis ETL | Every 60s |
| `archiving_dag` | HOT → WARM archiving | Daily |
| `cold_archiving_dag` | WARM → COLD archiving | Monthly |
| `analytics_dag` | Calculate KPIs | Every 60s |
| `data_quality_dag` | Data validation | Hourly |

**Note**: For the demo, we use `start_pipeline.sh` for easier control. DAGs are configured but can be enabled in Airflow UI for production-like automation.

---

## 🧪 Testing & Validation

### Verify Data Flow
```bash
# Check MongoDB data
docker exec fleet-mongodb mongosh -u admin -p admin123 --eval "
  use fleet_analytics;
  db.telemetry_events.countDocuments()
"

# Check Redis cache
docker exec fleet-redis redis-cli -a redis123 KEYS "cache:*"

# Monitor logs
tail -f logs/generator.log
tail -f logs/consumer.log
tail -f logs/redis_updater.log
```

### Data Quality Checks

- ✅ Telemetry events have valid GPS coordinates
- ✅ Speed values within realistic range (0-120 km/h)
- ✅ Fuel levels decrease over time
- ✅ No duplicate vehicle IDs in fleet
- ✅ All incidents linked to valid vehicles/drivers

---

## 📊 Key Implementation Details

### Redis Dual Purpose

Redis serves two critical functions:

1. **ETL Staging Area**: 
   - Extracts data from MongoDB
   - Stages for cleaning/transformation
   - Prepares for analytics

2. **Caching Layer**:
   - Stores calculated KPIs
   - Reduces database load
   - Enables fast dashboard queries

### Complex SQL Queries

The pipeline includes sophisticated aggregations:
```python
# Example: Active vehicles with recent telemetry
active_vehicles = db.telemetry_events.distinct('vehicle_id', 
    {'timestamp': {'$gte': five_minutes_ago}})

# Example: Delivery success rate
completed = db.deliveries.count_documents({'status': 'Completed'})
total = db.deliveries.count_documents({})
success_rate = (completed / total * 100) if total > 0 else 0
```

---

## 🛑 Stopping the Pipeline
```bash
# Graceful shutdown (recommended)
# Press Ctrl+C in the terminal running start_pipeline.sh

# Or force stop
./scripts/stop_pipeline.sh

# Stop Docker services
docker-compose down
```

---

## 📸 Screenshots

*Include screenshots of:*
- Grafana dashboard showing all 10 KPIs
- Airflow UI with 7 DAGs
- Data flowing in real-time
- Docker containers running

---

## 👥 Team Contributions

| Team Member | Contributions |
|-------------|---------------|
| Sameed Ahmad | Pipeline architecture, data generation, Kafka integration, Grafana dashboards |
| [Partner Name] | Spark processing, MongoDB schema, Airflow DAGs, documentation |

---

## 🎓 Academic Context

**Course**: Big Data Analytics  
**Institution**: [Your University]  
**Semester**: [Semester/Year]

### Learning Outcomes Demonstrated

- ✅ Big data pipeline design and implementation
- ✅ Real-time stream processing with Kafka/Spark
- ✅ NoSQL database design (MongoDB)
- ✅ Data caching strategies (Redis)
- ✅ Workflow orchestration (Airflow)
- ✅ Containerization with Docker
- ✅ Statistical modeling for data generation
- ✅ Data visualization with Grafana

---

## 📚 References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [MongoDB Aggregation Framework](https://docs.mongodb.com/manual/aggregation/)
- [Apache Spark Streaming](https://spark.apache.org/streaming/)
- [Grafana Documentation](https://grafana.com/docs/)
- [Apache Airflow](https://airflow.apache.org/docs/)


---

## 🤝 Acknowledgments

- Statistical models based on real-world fleet management patterns
- Dashboard design inspired by modern fleet management systems
- Thanks to open-source community for excellent tools

---

**Project Repository**: https://github.com/Sameed-Ahmad/fleet-analytics-pipeline

**Last Updated**: December 2025