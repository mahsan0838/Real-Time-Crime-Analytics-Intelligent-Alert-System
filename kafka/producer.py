import json
import time
import csv
from kafka import KafkaProducer
import os

# Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'crime_events'
CSV_PATH = '../data/Crimes_Sample_50k_clean.csv'

# Create producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Starting producer... sending to topic: {TOPIC}")

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
        
        # Wait 1 second between messages
        time.sleep(1)

producer.flush()
print(f"Done! Sent {count} messages")