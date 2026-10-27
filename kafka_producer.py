from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

while True:
    evento = {
        "sensor_id": random.randint(1, 5),
        "temperature": round(random.uniform(20, 35), 2),
        "humidity": round(random.uniform(40, 80), 2),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    producer.send("sensor_data", evento)
    print("Enviado:", evento)
    time.sleep(1)
