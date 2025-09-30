# FILE: database_writer.py

import json
from kafka import KafkaConsumer
from influxdb import InfluxDBClient
import time

print("--- Starting Database Writer ---")

# Give Kafka a moment to start up fully
time.sleep(15) 

# Connect to the InfluxDB database
db_client = InfluxDBClient('localhost', 8086, database='rul_predictions_db')
db_client.create_database('rul_predictions_db')
print("✅ Connected to InfluxDB.")

# Connect to Kafka to listen for predictions
consumer = KafkaConsumer(
    'prediction-stream', # The topic where the analyst sends results
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda message: json.loads(message.decode('utf-8')),
    auto_offset_reset='latest'
)
print("✅ Connected to Kafka. Listening for predictions...")

# Listen forever and write every prediction to the database
for message in consumer:
    prediction = message.value
    engine_id = prediction['engine_id']
    rul = prediction['predicted_rul']

    print(f"Logging to DB: Engine {engine_id}, RUL: {rul}")

    point = [{
        "measurement": "engine_rul",
        "tags": { "engine_id": engine_id },
        "fields": { "predicted_rul": float(rul) }
    }]
    db_client.write_points(point)