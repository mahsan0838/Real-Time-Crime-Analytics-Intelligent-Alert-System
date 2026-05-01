# Technical Report — Real-Time Crime Analytics & Intelligent Alert System (CS4109)

**Course**: Fundamentals of Big Data Analytics (CS4109)  
**Project**: Real-Time Crime Analytics & Intelligent Alert System  
**Deadline**: 11-05-26 11:59PM  
**Team name**: _[Fill]_  
**Members**: _[Fill Name + Roll No]_, _[Fill Name + Roll No]_  
**Instructor(s)**: Ms. Kainat Iqbal, Dr. Asif Muhammad  
**Teaching Assistants**: Moaz Murtaza, Bilal Bashir  

---

## Contents

1. Introduction  
2. Learning Objectives  
3. Dataset Description  
   3.1 Key Schema Fields  
   3.2 Dataset filenames used  
4. Problem Statement  
5. System Architecture  
   5.1 Batch Layer — Apache Spark  
   5.2 Speed Layer — Kafka + Apache Storm  
   5.3 Serving Layer — PostgreSQL & MongoDB  
   5.4 Infrastructure  
6. Required Streaming Analytics  
7. Additional Analysis  
   7.1 Crime Trend Analysis  
   7.2 Arrest Rate Analysis  
   7.3 Violence and Gunshot Analysis  
   7.4 Sex Offender Proximity Analysis  
   7.5 Geospatial Hotspot Detection (K-Means)  
   7.6 Cross-Dataset Correlation  
8. Execution Expectations  
9. Academic Integrity  
10. Deliverables  

---

## 1. Introduction

Urban crime analysis is a critical public safety problem where decisions often need to be made in near real time. Traditional batch-only reporting is insufficient for time-sensitive anomaly detection and alerting. This project implements a **Lambda Architecture** pipeline for the City of Chicago public safety datasets that supports:

- **Batch analytics (Spark)**: historical trends, joins, clustering (K-Means), and correlations.
- **Streaming analytics (Kafka + speed layer)**: simulated ingestion of crime events and window-based anomaly alerting.
- **Serving layer (PostgreSQL + MongoDB)**: persistence of analytics results and alert logs.
- **Dashboard (Streamlit, optional)**: live monitoring of alerts and summary metrics.

Scope constraint: streaming is simulated by replaying CSV rows at a controlled rate (default 1 row/second).

---

## 2. Learning Objectives

This implementation covers:

1. Lambda Architecture (batch + speed + serving).
2. Ingestion of heterogeneous CSV datasets.
3. **Explicit Spark schemas** (StructType; no inferSchema) using header-derived all-string schemas and safe casts.
4. Kafka producer and topics.
5. Implement multi-stage streaming processing with windowed anomaly detection and alert generation.
6. Apply unsupervised ML (K-Means clustering) for geospatial hotspot detection (**k = 10**).
7. Perform cross-dataset joins and correlations across heterogeneous schemas.
8. Persist analytics results to PostgreSQL and MongoDB and expose via a dashboard.
9. Containerise and orchestrate components using Docker Compose.
10. Communicate system design through structured technical documentation.

---

## 3. Dataset Description

All datasets were downloaded from the City of Chicago Open Data Portal.

- **Crime Data (2001–Present)**: `ijzp-q8t2`  
  Link: `https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2/about_data`
- **Police Stations**: `z8bn-74gv`  
  Link: `https://data.cityofchicago.org/Public-Safety/Police-Stations/z8bn-74gv/about_data`
- **Arrests**: `dpt3-jri9`  
  Link: `https://data.cityofchicago.org/Public-Safety/Arrests/dpt3-jri9/about_data`
- **Violence Reduction (Victims of Homicides & Non-Fatal Shootings)**: `gumc-mgzr`  
  Link: `https://data.cityofchicago.org/Public-Safety/Violence-Reduction-Victims-of-Homicides-and-Non-Fa/gumc-mgzr/about_data`
- **Sex Offenders**: `vc9r-bqvy`  
  Link: `https://data.cityofchicago.org/Public-Safety/Sex-Offenders/vc9r-bqvy/about_data`

### 3.1 Key Schema Fields

The most important fields used across datasets (nulls, type mismatches, and naming inconsistencies were handled in code):

- **Crime**: `CASE NUMBER`, `DATE`, `BLOCK`, `PRIMARY TYPE`, `DISTRICT`, `ARREST`, `LATITUDE`, `LONGITUDE`
- **Police Stations**: `DISTRICT`, `DISTRICT NAME`, `ADDRESS`, `ZIP`, `PHONE`, `LATITUDE`, `LONGITUDE`
- **Arrests**: `CB NO`, `CASE NUMBER`, `ARREST DATE`, `RACE`, `CHARGE 1..4 STATUTE/DESCRIPTION/TYPE/CLASS`
- **Violence Reduction**: `CASE NUMBER`, `DATE`, `MONTH`, `DAY OF WEEK`, `RACE`, `SEX`, `DISTRICT`, `COMMUNITY AREA`, `GUNSHOT INJURY I` (and homicide-related/FBI-code columns; export varies)
- **Sex Offenders**: `LAST`, `FIRST`, `BLOCK`, `GENDER`, `RACE`, `BIRTH DATE`, `HEIGHT`, `WEIGHT`, `VICTIM MINOR`

### 3.2 Dataset filenames used (from `config/config.yaml`)

- Crime: `data/Crimes_-_2001_to_Present_20260501.csv`
- Arrests: `data/Arrests_20260501.csv`
- Police stations: `data/Police_Stations_20260501.csv`
- Violence: `data/Violence_Reduction_-_Victims_of_Homicides_and_Non-Fatal_Shootings_20260501.csv`
- Sex offenders: `data/Sex_Offenders_20260501.csv`

---

## 4. Problem Statement

Chicago’s public safety ecosystem generates large volumes of records across crime reports, arrests, violence incidents, and sex offender registrations. These datasets are heterogeneous and inconsistent in schema, and they are often processed in isolation using static tools.

The goal of this project is to design and implement a unified, real-time crime analytics platform that:

- Ingests all five Chicago public safety datasets.
- Streams crime events row-by-row through a Kafka producer simulating live incident feeds.
- Detects anomalies in real time using sliding time windows and district-level event counts.
- Generates alerts when district thresholds are breached and persists them.
- Performs batch analytics in Spark (trends, arrest rates, joins, correlations, K-Means hotspots).
- Persists outputs to PostgreSQL (structured) and MongoDB (semi-structured alert logs).
- Visualises key metrics in an optional Streamlit dashboard.

---

## 5. System Architecture

### 5.1 Batch Layer — Apache Spark

The batch layer reads all five datasets from disk, enforces explicit schemas, performs preprocessing and analytics, and writes results to PostgreSQL.

- **Main batch job**: `spark/run_batch_layer.py`
- **Explicit schemas**: `spark/schemas.py` uses a header-derived `StructType` (all `StringType`) and then applies safe parsing/casting

### 5.2 Speed Layer — Kafka + Apache Storm

- **Kafka producer** streams crime rows as JSON to `crime_events` at a configurable rate (default 1 row/sec).
- **Expected Storm topology (per rubric)**:
  - KafkaSpout → ParseBolt → DistrictBolt → WindowBolt → AnomalyBolt → AlertBolt

**Implementation status**:
- A real multi-bolt Storm topology is implemented and submitted via Flux (`storm/flux/crime_topology.yaml`) using KafkaSpout + Python ShellBolts (Parse → District → Window → Anomaly → Alert).
- The legacy script `storm/anomaly_detector.py` remains available as a standalone speed-layer path for debugging, but the assessed Storm requirement is satisfied by the submitted topology visible in Storm UI.

### 5.3 Serving Layer — PostgreSQL & MongoDB

- **PostgreSQL** stores structured analytics outputs and alerts.
- **MongoDB** stores semi-structured alert logs with full JSON payload (`crime_analytics.alert_logs`).

### 5.5 Architecture diagram (insert)

**TODO (required)**: Insert a diagram showing Kafka → Storm/speed layer → PostgreSQL/Mongo, and Spark batch → PostgreSQL, plus Streamlit reading from DBs.

---

### 5.4 Infrastructure (Docker Compose)

All components are containerized and started via one command:

```bash
docker compose -f docker/docker-compose.yml up -d
```

Services included:

- Zookeeper
- Kafka
- PostgreSQL
- MongoDB
- Storm: Nimbus, Supervisor, UI
- Spark

**Windows note**: Docker PostgreSQL is mapped to host port **5433** to avoid conflict with an existing local PostgreSQL instance on 5432.

**TODO screenshots**:
- `docker ps` output showing all services running.
- Storm UI page (port 8080).
- Spark UI page (port 8081).

---

### 5.6 Configuration Management

All parameters are externalized in `config/config.yaml`, including:
- Kafka broker, topics, and producer rate
- Window length, slide interval, and anomaly threshold
- DB connection details
- Dataset file paths

**TODO screenshot**: `config/config.yaml` snippet.

---

## 6. Required Streaming Analytics

### 6.1 Kafka Producer (Crime Simulator)

- Script: `kafka/producer.py`
- Topic: `crime_events`
- Rate: configurable via `config/config.yaml` (`kafka.producer_rate`)
- Emitted JSON includes the required fields: case number, date, block, primary type, district, arrest, latitude, longitude.

**TODO screenshot**: terminal showing producer sending messages.

### 6.2 Storm topology (multi-bolt) and alerting

Per requirements, the speed layer should be a **multi-bolt Apache Storm topology** consuming Kafka:

1. **KafkaSpout**: reads from `crime_events`
2. **ParseBolt**: deserialises JSON, validates required fields, discards malformed messages (log and continue)
3. **DistrictBolt**: groups/keys tuples by `DISTRICT`
4. **WindowBolt**: maintains sliding count window (configurable window length + slide interval)
5. **AnomalyBolt**: compares counts against threshold; emits anomaly tuples with district, count, threshold, timestamp
6. **AlertBolt**: persists alerts to MongoDB + PostgreSQL (and optionally emits to `crime_alerts`)

**Current implementation status**:
- The topology is submitted to Storm (Nimbus/Supervisor) via `scripts/submit_storm_topology.sh` and is visible in Storm UI.
- Bolts are implemented as Python ShellBolts using Storm’s multilang JSON protocol.

**What to put in the final PDF (screenshots)**:
- Storm UI showing the submitted topology graph (spout + bolts)
- Storm UI showing worker/executor stats and emitted tuples

### 6.3 Current windowed anomaly detection and alert persistence (working path)

- Script: `storm/anomaly_detector.py`
- Window size and threshold are configurable via `config/config.yaml` (minutes converted to seconds).
- Alerts are written to:
  - PostgreSQL table: `alerts`
  - MongoDB collection: `crime_analytics.alert_logs`
  - Kafka output topic: `crime_alerts`

**Current serving-layer evidence (PostgreSQL)**:
- `alerts`: **48 rows**

**Current serving-layer evidence (MongoDB)**:
- `alert_logs`: **0 documents**  
  **TODO (fix before final submission)**: run `storm/anomaly_detector.py` while producer runs, then capture MongoDB count + one sample document screenshot.

---

## 7. Additional Analysis

Batch layer is executed in Docker Spark to avoid Windows Spark/Java issues:

```bash
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'
docker exec -i spark /opt/spark/bin/spark-submit /opt/spark/app/run_batch_layer.py
```

### 7.0 Schema enforcement (no inference)

We enforce explicit Spark schemas for each dataset using a header-derived, all-string `StructType`, then apply safe parsing/casting.

- Schema helper: `spark/schemas.py` (`string_schema_from_csv_header`)

### 7.1 Crime Trend Analysis

Aggregations grouped by: **year, month, day of week, hour of day**.  
Stored in PostgreSQL: `crime_trends`.

**Evidence**:
- `crime_trends`: **51,072 rows**

### 7.2 Arrest Rate Analysis (Join on CASE NUMBER)

Crimes are joined with Arrests on **CASE NUMBER**, then arrest rates are computed by:
- primary crime type (`crime_type`)
- police district (`district`)
- race (`race`)

Stored in PostgreSQL: `arrest_rates`.

**Evidence**:
- `arrest_rates`: **3,435 rows**

Example top rows (by arrest rate):

| crime_type | district | race | arrest_rate |
|---|---:|---|---:|
| BATTERY | 4 | BLACK HISPANIC | 100 |
| ROBBERY | 19 | BLACK | 100 |
| PROSTITUTION | 1 | ASIAN / PACIFIC ISLANDER | 100 |
| CONCEALED CARRY LICENSE VIOLATION | 10 | WHITE HISPANIC | 100 |
| DECEPTIVE PRACTICE | 16 | BLACK HISPANIC | 100 |

### 7.3 Violence and Gunshot Analysis

Planned outputs (per requirements):
- total homicides vs non-fatal shootings by month and district
- proportion of gunshot injury incidents (`GUNSHOT INJURY I = 'Y'`)
- top community areas by violence incidents

Stored in PostgreSQL: `violence_stats`.

**Current status**:
- `violence_stats`: **0 rows**  
**TODO (fix before final submission)**: adjust violence homicide field mapping to match export columns, rerun batch layer and include screenshots/queries.

### 7.4 Sex Offender Proximity Analysis

Required: density of registered offenders by district; highlight `VICTIM MINOR = 'Y'`.

Stored in PostgreSQL: `sex_offender_density`.

**Evidence**:
- `sex_offender_density`: **1 row**
- Current row (district NULL due to dataset export lacking `DISTRICT`):
  - offender_count = **3951**
  - victim_minor_count = **3052**

**Limitation (must state in viva/report)**:
The downloaded Sex Offenders export used here does not contain a `DISTRICT` field, so true “density by district” cannot be computed without an additional mapping step (e.g., geocoding block to coordinates and spatially joining to districts/stations).

### 7.5 Geospatial Hotspot Detection (K-Means, k=10)

We run K-Means with **k=10** over latitude/longitude (after filtering valid coordinates).  
Stored in PostgreSQL: `hotspots` containing centroid and cluster crime_count.

**Evidence**:
- `hotspots`: **10 rows**

Top centroids (by crime_count):

| cluster_id | latitude | longitude | crime_count |
|---:|---:|---:|---:|
| 0 | 41.7791818330 | -87.6478794039 | 1336184 |
| 9 | 41.8796988815 | -87.6385304075 | 1176316 |
| 4 | 41.8763610744 | -87.7062755614 | 1159473 |
| 3 | 41.9079366927 | -87.7470659605 | 1046832 |
| 7 | 41.7564205168 | -87.5835662684 | 1042106 |

### 7.6 Cross-Dataset Correlation (2+)

Stored in PostgreSQL: `correlations`:

| correlation_type | value |
|---|---:|
| violence_vs_arrest_rate_by_district | 0.6464922822256346 |
| sex_offender_density_vs_crime_count_by_district | NaN |

Note: the second correlation is NaN because sex offender density is not district-resolved in the current export (district is NULL).

---

## 9. Serving Layer (PostgreSQL + MongoDB)

### 9.1 PostgreSQL tables used

- `crime_trends`: batch trend aggregates
- `arrest_rates`: joined arrest rate breakdown by crime_type/district/race
- `hotspots`: K-Means centroids and cluster sizes
- `correlations`: correlation results
- `alerts`: triggered streaming alerts
- `violence_stats`: violence analytics (currently empty; TODO)
- `sex_offender_density`: sex offender density (limited by export; TODO)

**TODO screenshots**:
- `\dt` (list tables) and sample rows from each.

### 9.2 MongoDB collections used

- `crime_analytics.alert_logs`: alert logs (required)

**TODO screenshots**:
- countDocuments
- one sample alert document

---

## 10. Dashboard (Optional Bonus)

Command:

```bash
streamlit run dashboard/app.py
```

The dashboard reads DB configuration from `config/config.yaml` and shows alert metrics and arrest-rate charts.

**TODO screenshot**: dashboard page.

---

## 11. Reproducibility (Exact Run Instructions)

### 11.1 Start infrastructure

```bash
docker compose -f docker/docker-compose.yml up -d
```

### 11.2 Create Kafka topics

```bash
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'
docker exec -i kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic crime_events --partitions 1 --replication-factor 1
docker exec -i kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic crime_alerts --partitions 1 --replication-factor 1
```

### 11.3 Run streaming

Terminal A:
```bash
venv/Scripts/python storm/anomaly_detector.py
```

Terminal B:
```bash
venv/Scripts/python kafka/producer.py
```

### 11.4 Run batch layer (Spark container)

```bash
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'
docker exec -i spark /opt/spark/bin/spark-submit /opt/spark/app/run_batch_layer.py
```

### 11.5 Run dashboard (optional)

```bash
streamlit run dashboard/app.py
```

---

## 12. Challenges and Fixes

- **Windows Spark/Java issues**: Spark jobs are executed inside the Docker Spark container.
- **Git Bash path conversion**: use `MSYS_NO_PATHCONV=1` for `docker exec` with Linux paths.
- **PostgreSQL port conflict on 5432**: Docker PostgreSQL mapped to host port **5433**.
- **Dirty/variable CSV schemas**: enforced explicit, header-derived StructType schemas and safe casting.

---

## 13. Conclusion

The system demonstrates a complete end-to-end big-data pipeline with both batch and streaming components. Batch outputs for `crime_trends`, `arrest_rates`, `hotspots`, and `correlations` are successfully persisted in PostgreSQL, and streaming alerts are persisted in PostgreSQL. Remaining work before final submission is to ensure MongoDB alert logging is populated and to finalize the violence/sex offender district-level reporting depending on dataset field availability.

---

## 8. Execution Expectations (Checklist)

1. **Single-command launch**: `docker compose -f docker/docker-compose.yml up -d` starts Kafka/Zookeeper/Storm/Spark/PostgreSQL/MongoDB.
2. **Reproducibility**:
   - Streaming producer runnable via `kafka/producer.py`
   - Speed layer runnable via `storm/anomaly_detector.py` (current path)  
   - Batch layer runnable via Spark container submit of `spark/run_batch_layer.py`
3. **Schema enforcement**: Spark uses explicit `StructType` (no `inferSchema=True`).
4. **Error handling**:
   - Producer handles CSV read issues and continues
   - Streaming path logs malformed messages (when encountered) and continues
5. **Configuration externalisation**: broker/topic/window/threshold/DB credentials are in `config/config.yaml`.
6. **Data volume**:
   - Crime: ≥ 50,000 rows (or full dataset)
   - Other datasets: ≥ 10,000 rows each (or full dataset)
7. **Dashboard (optional)**: Streamlit dashboard screenshots (alerts + trends + hotspot map).

---

## 9. Academic Integrity / AI Disclosure

**TODO (required)**: Add your group’s disclosure statement here.

Example text you can paste (edit to be truthful):

- We used AI assistance (ChatGPT/Cursor) for debugging, refactoring, and documentation drafting. All code was reviewed, executed, and verified by the team. We understand and can explain all submitted components during viva.
- No code was shared between groups. External references were limited to official documentation and public tutorials for Kafka/Spark/Storm/MongoDB/PostgreSQL.

**TODO screenshots (optional bonus per instructions)**:
- Screenshot of a representative prompt (and output) used during development.

---

## 10. Deliverables (Zip Contents Checklist)

Your submission `.zip` should include:

1. **Source code (60%)**:
   - `spark/`, `kafka/`, `storm/`, `dashboard/`, `db/`, `config/`, `docker/`, `scripts/`, `requirements.txt`, `README.md`
2. **Docker setup (5%)**:
   - `docker/docker-compose.yml`
   - Dockerfiles (`docker/Dockerfile.spark`, etc.)
3. **Technical report PDF ≤ 20 pages (15%)**:
   - Export this `REPORT.md` to PDF after inserting required screenshots/figures
4. **Demo video 5–8 minutes (15%)**:
   - **TODO**: Insert a link here (Google Drive/OneDrive) to your screen recording, and also place the file in the zip if required by your portal.
   - Must show: Kafka streaming, Storm UI (topology), Spark job completion, PostgreSQL + MongoDB query results
5. **Contribution statement (5%)**:
   - **TODO**: Add `CONTRIBUTION.md` (or signed PDF/image) with % breakdown per member and component

