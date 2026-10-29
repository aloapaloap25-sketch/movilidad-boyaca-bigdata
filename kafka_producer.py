from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Enviando datos...")
while True:
    data = {
        'sensor_id': random.randint(1, 10),
        'temperature': round(random.uniform(20, 30), 2)
    }
    producer.send('sensor_data', value=data)
    print(f"Sent: {data}")
    time.sleep(1)
