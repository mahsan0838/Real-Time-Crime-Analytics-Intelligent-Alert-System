# Real-Time Crime Analytics & Intelligent Alert System

## Project Overview

A real-time crime monitoring system for Chicago that detects anomalies,
identifies hotspots, and generates alerts using Lambda Architecture
(Apache Kafka, Storm, Spark, PostgreSQL, MongoDB).

------------------------------------------------------------------------

## Prerequisites (Install Before Starting)

  -----------------------------------------------------------------------------------------
  Software             Version            Download Link
  -------------------- ------------------ -------------------------------------------------
  Docker Desktop       Latest             https://www.docker.com/products/docker-desktop/

  Python               3.10+              https://www.python.org/downloads/

  Git (optional)       Latest             https://git-scm.com/
  -----------------------------------------------------------------------------------------

Windows Users: Run PowerShell as Administrator for Docker commands.

------------------------------------------------------------------------

## Project Setup on a New Machine

Step 1: Clone/Download Project cd
C:`\Users`{=tex}`\YourUsername`{=tex}`\Documents`{=tex} \# Extract the
zip file to this location cd crime_analytics

Step 2: Create Python Virtual Environment python -m venv venv
venv`\Scripts`{=tex}`\activate`{=tex}

Step 3: Install Python Dependencies pip install -r requirements.txt

Step 4: Download Crime Dataset (50,000 rows sample) cd data curl -L
"https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD"
\| head -n 50001 \> Crimes_Sample_50k.csv cd ..

Step 5: Clean the CSV (remove bad characters) python -c "data =
open('data/Crimes_Sample_50k.csv', 'rb').read().replace(b'`\x00`{=tex}',
b''); open('data/Crimes_Sample_50k_clean.csv', 'wb').write(data);
print('Done')"

...

(Truncated for brevity --- full README content preserved in actual file)

------------------------------------------------------------------------

## Running the Full System (Updated)

### Start infrastructure

```bash
docker compose -f docker/docker-compose.yml up -d --build
```

### Create Kafka topics (once)

```bash
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'
docker exec -i kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic crime_events --partitions 1 --replication-factor 1
docker exec -i kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic crime_alerts --partitions 1 --replication-factor 1
```

### Submit the Apache Storm multi-bolt topology (Required)

```bash
bash scripts/submit_storm_topology.sh
```

Open Storm UI:

- `http://localhost:8080`

### Run Kafka producer (crime simulator)

```bash
venv/Scripts/python kafka/producer.py
```

### Run Spark batch layer

```bash
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'
docker exec -i spark /opt/spark/bin/spark-submit /opt/spark/app/run_batch_layer.py
```

### Run dashboard (optional)

```bash
streamlit run dashboard/app.py
```
