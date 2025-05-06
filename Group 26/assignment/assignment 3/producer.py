import json
import random
from datetime import datetime
from time import sleep
from kafka import KafkaProducer

# Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'traffic_data'
SENSORS = ['S101', 'S102', 'S103', 'S104', 'S105']

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_congestion(vehicle_count):
    if vehicle_count > 30:
        return "HIGH"
    elif vehicle_count > 15:
        return "MEDIUM"
    else:
        return "LOW"

while True:
    sensor_id = random.choice(SENSORS)
    vehicle_count = random.randint(0, 50)
    avg_speed = random.uniform(5, 100)  # Ensure speed > 0
    timestamp = datetime.now().isoformat()
    congestion_level = generate_congestion(vehicle_count)
    
    event = {
        "sensor_id": sensor_id,
        "timestamp": timestamp,
        "vehicle_count": vehicle_count,
        "average_speed": avg_speed,
        "congestion_level": congestion_level
    }
    
    producer.send(TOPIC, event)
    print(f"Sent: {event}")
    sleep(1)