# Real-Time Crime Analytics & Intelligent Alert System

## Project Overview
A real-time crime monitoring system for Chicago that detects anomalies, identifies hotspots, and generates alerts using Lambda Architecture (Apache Kafka, Storm, Spark, PostgreSQL, MongoDB).

## ✅ Current Status - COMPLETE
- All 8 Docker containers running (Kafka, Zookeeper, PostgreSQL, MongoDB, Storm Nimbus/Supervisor/UI, Spark)
- Kafka topic `crime_events` created and streaming
- PostgreSQL tables created (alerts, arrest_rates, correlations, crime_trends, hotspots)
- MongoDB storing alerts in `alert_logs` collection
- **Anomaly Detection:** Working (60-second window, threshold 5 crimes)
- **Spark Batch Analytics:** Crime trends, arrest rates, K-Means hotspots
- Kafka producer streaming 50,000 crime records successfully

---

## Quick Start After a Break

### 1. Start All Containers
```bash
cd C:\Users\ahsan\Documents\Ahsan's Drive\8th\Big Data\proj\crime_analytics\docker
docker-compose up -d
2. Verify All Containers Are Running
bash
docker-compose ps
You should see 8 containers:

kafka (Up)

zookeeper (Up)

postgres (Up)

mongodb (Up)

storm_nimbus (Up)

storm_supervisor (Up)

storm_ui (Up)

spark (Up)

3. Verify Kafka Topic Exists
bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
Should show: crime_events

4. Verify PostgreSQL Tables
bash
docker exec -it postgres psql -U crime_user -d crime_analytics -c "\dt"
Should show 5 tables: alerts, arrest_rates, correlations, crime_trends, hotspots

5. Verify MongoDB Alerts Collection
bash
docker exec -it mongodb mongosh --eval "use crime_analytics; db.alert_logs.countDocuments()"
6. Activate Python Virtual Environment
bash
cd C:\Users\ahsan\Documents\Ahsan's Drive\8th\Big Data\proj\crime_analytics
venv\Scripts\activate
7. Run Kafka Producer (starts streaming)
bash
cd kafka
python producer.py
Expected output: [1] Sent: JG503434 - OFFENSE INVOLVING CHILDREN

8. Run Anomaly Detector (NEW TERMINAL)
bash
cd C:\Users\ahsan\Documents\Ahsan's Drive\8th\Big Data\proj\crime_analytics
venv\Scripts\activate
cd storm
python anomaly_detector.py
Expected output: 🚨 ALERT: District 008 - 5 crimes in last 60s

Container Access Commands
Service	Command
PostgreSQL	docker exec -it postgres psql -U crime_user -d crime_analytics
MongoDB	docker exec -it mongodb mongosh
Kafka	docker exec -it kafka bash
Spark	docker exec -it spark bash
Storm UI	Open browser: http://localhost:8080
Spark UI	Open browser: http://localhost:8081
File Locations
Component	Path
Docker config	crime_analytics/docker/docker-compose.yml
App config	crime_analytics/config/config.yaml
Database init	crime_analytics/db/postgres_init.sql
Kafka producer	crime_analytics/kafka/producer.py
Anomaly detector	crime_analytics/storm/anomaly_detector.py
Batch analytics	crime_analytics/spark/batch_analytics_clean.py
K-Means hotspots	crime_analytics/spark/kmeans_hotspots.py
Crime data	crime_analytics/data/Crimes_Sample_50k_clean.csv
Running Spark Batch Analytics
Crime Trends & Arrest Rates
bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/app/batch_analytics_clean.py
K-Means Hotspot Detection
bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/app/kmeans_hotspots.py
Expected K-Means Output:

text
=== Crime Hotspot Centers (Latitude, Longitude) ===
Hotspot 1: (41.750318, -87.626822)
Hotspot 2: (36.619446, -91.686566)
Hotspot 3: (41.965193, -87.679379)
Hotspot 4: (41.868528, -87.650413)
Hotspot 5: (41.895945, -87.746848)
Viewing Results
Check Alerts in PostgreSQL
bash
docker exec -it postgres psql -U crime_user -d crime_analytics -c "SELECT * FROM alerts ORDER BY id DESC LIMIT 10;"
Check Alerts in MongoDB
bash
docker exec -it mongodb mongosh --eval "use crime_analytics; db.alert_logs.find().limit(5).pretty()"
Check Kafka Output Topic (Alerts)
bash
docker exec -it kafka kafka-console-consumer --topic crime_alerts --bootstrap-server localhost:9092 --from-beginning --max-messages 3
System Architecture Summary
Layer	Component	Technology	Status
Speed Layer	Event Streaming	Kafka Producer	✅
Speed Layer	Anomaly Detection	Apache Storm	✅
Speed Layer	Alerts Storage	PostgreSQL + MongoDB	✅
Batch Layer	Trends & Arrest Rates	PySpark	✅
Batch Layer	Hotspot Detection	K-Means (MLlib)	✅
Serving Layer	Structured Data	PostgreSQL	✅
Serving Layer	Semi-structured Data	MongoDB	✅
Infrastructure	Orchestration	Docker Compose	✅
Common Issues & Fixes
Problem	Solution
Container won't start	docker-compose down -v && docker-compose up -d
Kafka topic missing	docker exec -it kafka kafka-topics --create --topic crime_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
PostgreSQL tables missing	docker restart postgres
MongoDB shows 0 alerts	Uninstall local MongoDB (conflict on port 27017)
CSV encoding error	Clean CSV saved as Crimes_Sample_50k_clean.csv
Port conflicts	Change port numbers in docker-compose.yml
Spark numpy error	Already fixed in Dockerfile.spark


Stop Everything (When Done for the Day)
bash
cd docker
docker-compose down
To stop AND delete all data (fresh start next time):

bash
docker-compose down -v
Performance Metrics (50,000 Crime Records)
Metric	Value
Total crimes processed	50,000
Top crime type	THEFT (10,882)
Highest arrest rate	NARCOTICS (97.65%)
Highest crime district	District 008 (3,416 crimes)
Highest arrest rate district	District 011 (23.89%)
K-Means clusters	5 hotspots
Anomaly threshold	5 crimes/60 seconds




curl -o postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
docker cp postgresql-42.6.0.jar spark:/opt/spark/jars/


docker cp populate_tables.py spark:/opt/spark/app/
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/app/populate_tables.py