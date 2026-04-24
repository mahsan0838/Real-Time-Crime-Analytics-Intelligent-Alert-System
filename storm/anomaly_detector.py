from kafka import KafkaConsumer, KafkaProducer
import json
import time
from collections import defaultdict
from datetime import datetime, timedelta
import psycopg2
from pymongo import MongoClient
from bson import ObjectId

# Configuration
KAFKA_BROKER = 'localhost:9092'
INPUT_TOPIC = 'crime_events'
OUTPUT_TOPIC = 'crime_alerts'
WINDOW_SECONDS = 60
THRESHOLD = 5

# PostgreSQL connection
pg_conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='crime_analytics',
    user='crime_user',
    password='crime_pass'
)
pg_cursor = pg_conn.cursor()

# MongoDB connection
# MongoDB connection
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client.crime_analytics
mongo_alerts = mongo_db.alert_logs
print("MongoDB connected to crime_analytics.alert_logs")

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
        
        print(f"\n🚨 ALERT: District {district} - {count} crimes in last {WINDOW_SECONDS}s")
        
        # Save to PostgreSQL
        pg_cursor.execute(
            "INSERT INTO alerts (district, timestamp, event_count, threshold, severity) VALUES (%s, %s, %s, %s, %s)",
            (int(district), now, count, THRESHOLD, 'HIGH')
        )
        pg_conn.commit()
        
                # Save to MongoDB
        try:
            # Ensure database and collection exist
            db = mongo_client['crime_analytics']
            collection = db['alert_logs']
            result = collection.insert_one(alert)
            print(f"Saved to MongoDB with id: {result.inserted_id}")
            # Verify it was saved
            count = collection.count_documents({})
            print(f"Total documents in MongoDB now: {count}")
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