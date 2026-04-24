from storm import Spout, Bolt, Topology
import json

class KafkaSpout(Spout):
    def initialize(self, conf, context):
        self.count = 0
        
    def next_tuple(self):
        # For now, we'll read from a file to simulate
        # Real implementation would read from Kafka
        import time
        time.sleep(1)
        return None

class ParseBolt(Bolt):
    def process(self, tup):
        try:
            message = json.loads(tup.values[0])
            # Validate required fields
            if message.get('case_number') and message.get('district'):
                self.emit([message])
            else:
                print(f"Malformed message: {message}")
        except:
            print(f"Parse error on: {tup.values[0]}")

class DistrictBolt(Bolt):
    def process(self, tup):
        crime = tup.values[0]
        district = crime.get('district', '000')
        self.emit([district, crime])

class WindowBolt(Bolt):
    def initialize(self, conf, context):
        self.window = {}  # {district: count}
        
    def process(self, tup):
        district = tup.values[0]
        self.window[district] = self.window.get(district, 0) + 1
        self.emit([district, self.window[district]])

class AnomalyBolt(Bolt):
    def initialize(self, conf, context):
        self.threshold = 100
        
    def process(self, tup):
        district, count = tup.values[0], tup.values[1]
        if count > self.threshold:
            self.emit([district, count, self.threshold])

class AlertBolt(Bolt):
    def process(self, tup):
        district, count, threshold = tup.values[0], tup.values[1], tup.values[2]
        import datetime
        alert = {
            'district': district,
            'timestamp': str(datetime.datetime.now()),
            'event_count': count,
            'threshold': threshold,
            'severity': 'HIGH'
        }
        print(f"ALERT: {alert}")
        # Save to PostgreSQL and MongoDB would go here

if __name__ == '__main__':
    # For now, just test locally
    print("Storm topology ready - run with storm jar command")