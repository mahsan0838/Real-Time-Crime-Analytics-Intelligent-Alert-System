from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'crime_events',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening for crime events... Press Ctrl+C to stop")

for message in consumer:
    crime = message.value
    district = crime.get('district', 'unknown')
    crime_type = crime.get('primary_type', 'unknown')
    print(f"[{district}] {crime_type} - {crime.get('case_number')}")