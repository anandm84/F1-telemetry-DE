from kafka import KafkaConsumer
import json
import os

consumer = KafkaConsumer(
    'f1_lap_times',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

os.makedirs("data/bronze", exist_ok=True)

for message in consumer:
    lap_data = message.value
    
    with open("data/bronze/laps.json", "a") as f:
        f.write(json.dumps(lap_data) + "\n")

    print("Written to bronze")