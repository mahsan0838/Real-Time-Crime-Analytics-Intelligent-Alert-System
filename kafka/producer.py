import json
import time
import csv
from kafka import KafkaProducer
import os
from pathlib import Path

import yaml

def _load_config():
    # project_root/kafka/producer.py -> project_root
    project_root = Path(__file__).resolve().parents[1]
    cfg_path = project_root / "config" / "config.yaml"
    if not cfg_path.exists():
        return project_root, {}
    return project_root, yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}


def _resolve_csv_path(project_root: Path, cfg: dict) -> str:
    # Prefer a sample file if present, otherwise fall back to config.yaml path.
    candidates = [
        project_root / "data" / "Crimes_Sample_50k_clean.csv",
        project_root / "data" / "Crimes_Sample_50k.csv",
    ]
    cfg_rel = (((cfg or {}).get("data_paths") or {}).get("crime") or "").strip()
    if cfg_rel:
        candidates.append(project_root / cfg_rel)

    for p in candidates:
        if p.exists():
            return str(p)

    # last resort: keep previous relative behavior
    return str(project_root.parent / "data" / "Crimes_Sample_50k_clean.csv")


# Configuration (from config/config.yaml when available)
PROJECT_ROOT, CFG = _load_config()
KAFKA_BROKER = ((CFG.get("kafka") or {}).get("broker")) or "localhost:9092"
TOPIC = ((CFG.get("kafka") or {}).get("topic")) or "crime_events"
RATE = int(((CFG.get("kafka") or {}).get("producer_rate")) or 1)  # rows/sec
CSV_PATH = _resolve_csv_path(PROJECT_ROOT, CFG)

# Create producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Starting producer... sending to topic: {TOPIC}")
print(f"Kafka broker: {KAFKA_BROKER}")
print(f"CSV path: {CSV_PATH}")

# Try different encodings
encodings = ['utf-8', 'latin-1', 'cp1252', 'ISO-8859-1']
reader = None
file_encoding = None

for enc in encodings:
    try:
        f = open(CSV_PATH, 'r', encoding=enc)
        reader = csv.DictReader(f)
        # Test read first row
        next(reader)
        f.seek(0)  # Reset to beginning
        next(reader)  # Skip header
        file_encoding = enc
        print(f"Successfully opened with encoding: {enc}")
        break
    except (UnicodeDecodeError, StopIteration):
        continue

if reader is None:
    print("Could not read CSV file with any encoding")
    exit(1)

# Read and stream CSV
with open(CSV_PATH, 'r', encoding=file_encoding) as f:
    reader = csv.DictReader(f)
    count = 0
    for row in reader:
        # Extract required fields
        message = {
            'case_number': row.get('Case Number', ''),
            'date': row.get('Date', ''),
            'block': row.get('Block', ''),
            'primary_type': row.get('Primary Type', ''),
            'district': row.get('District', ''),
            'arrest': row.get('Arrest', ''),
            'latitude': row.get('Latitude', ''),
            'longitude': row.get('Longitude', '')
        }
        
        # Send to Kafka
        producer.send(TOPIC, message)
        count += 1
        print(f"[{count}] Sent: {message['case_number']} - {message['primary_type']}")
        
        # Wait between messages
        time.sleep(1 / max(RATE, 1))

producer.flush()
print(f"Done! Sent {count} messages")