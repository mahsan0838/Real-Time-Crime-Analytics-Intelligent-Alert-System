import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import json
from collections import defaultdict
from datetime import datetime, timedelta
import psycopg2
from pymongo import MongoClient
from bson import ObjectId
from kafka import KafkaConsumer, KafkaProducer

from config.runtime import get_kafka, get_mongo, get_postgres, get_storm, load_config

CFG = load_config()
KAFKA = get_kafka(CFG)
STORM = get_storm(CFG)
PG = get_postgres(CFG)
MONGO = get_mongo(CFG)

KAFKA_BROKER = KAFKA.broker
INPUT_TOPIC = KAFKA.topic_in
OUTPUT_TOPIC = KAFKA.topic_out
WINDOW_SECONDS = STORM.window_seconds
THRESHOLD = STORM.anomaly_threshold

# PostgreSQL connection
pg_conn = psycopg2.connect(
    host=PG.host,
    port=PG.port,
    database=PG.database,
    user=PG.user,
    password=PG.password,
)
pg_cursor = pg_conn.cursor()

# MongoDB connection
mongo_client = MongoClient(f"mongodb://{MONGO.host}:{MONGO.port}/")
mongo_db = mongo_client[MONGO.database]
mongo_alerts = mongo_db[MONGO.collection_alerts]
print(f"MongoDB connected to {MONGO.database}.{MONGO.collection_alerts}")

# Custom JSON encoder to handle ObjectId
class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)

# Kafka consumer/producer
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, cls=JSONEncoder).encode('utf-8')
)

window = defaultdict(list)
alerted_districts = set()  # Avoid duplicate alerts per window

print(f"Kafka broker: {KAFKA_BROKER}")
print(f"Kafka topics: {INPUT_TOPIC} -> {OUTPUT_TOPIC}")
print(f"Monitoring crimes. Window: {WINDOW_SECONDS}s, Threshold: {THRESHOLD}")
print("Press Ctrl+C to stop")

for message in consumer:
    crime = message.value
    district = crime.get('district', '000')
    now = datetime.now()
    
    # Add to window
    window[district].append((now, crime))
    
    # Remove old entries
    cutoff = now - timedelta(seconds=WINDOW_SECONDS)
    window[district] = [(ts, c) for ts, c in window[district] if ts > cutoff]
    
    # Check threshold
    count = len(window[district])
    if count >= THRESHOLD and district not in alerted_districts:
        alerted_districts.add(district)
        
        alert = {
            'district': int(district),
            'timestamp': now.isoformat(),
            'event_count': count,
            'threshold': THRESHOLD,
            'severity': 'HIGH'
        }
        
        print(f"\nALERT: District {district} - {count} crimes in last {WINDOW_SECONDS}s")
        
        # Save to PostgreSQL
        pg_cursor.execute(
            "INSERT INTO alerts (district, timestamp, event_count, threshold, severity) VALUES (%s, %s, %s, %s, %s)",
            (int(district), now, count, THRESHOLD, 'HIGH')
        )
        pg_conn.commit()
        
        # Save to MongoDB
        try:
            result = mongo_alerts.insert_one(alert)
            print(f"Saved to MongoDB with id: {result.inserted_id}")
        except Exception as e:
            print(f"MongoDB error: {e}")
        
        # Send to Kafka output topic
        producer.send(OUTPUT_TOPIC, alert)
        
        # Reset after 10 seconds (for testing)
        import threading
        def reset_alert():
            import time
            time.sleep(10)
            alerted_districts.discard(district)
        threading.Thread(target=reset_alert).start()