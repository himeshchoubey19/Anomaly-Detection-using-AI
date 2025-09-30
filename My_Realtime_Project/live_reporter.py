# FILE: live_reporter.py
# This script's only job is to pretend to be a live engine.
# It reads the test data and sends it out, one line at a time.

# We need these tools to handle data tables, time, and messaging.
import pandas as pd
import time
import json
from kafka import KafkaProducer

print("--- Starting the Live Reporter Script ---")

# --- Step 1: Connect to the "Post Office" (Kafka) ---
# We will try to connect to the Kafka server. If it fails, we'll print a helpful error.
try:
    # This creates the "producer" that sends messages to Kafka.
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda data: json.dumps(data).encode('utf-8')
    )
    print("✅ Step 1: Successfully connected to Kafka.")
except Exception as e:
    print("❌ ERROR: Could not connect to Kafka.")
    print("➡ FIX: Please make sure your Docker command ('docker-compose up') is running in another terminal.")
    exit() # Stop the script if we can't connect.

# This is the name of the "channel" or "topic" we will send messages to.
KAFKA_TOPIC = 'sensor-stream'

# --- Step 2: Load the Data to be Streamed ---
# We will try to load the test data file. If it's missing, we'll print an error.
try:
    column_names = ['engine_id', 'cycle', 'setting_1', 'setting_2', 'setting_3'] + [f'sensor_{i}' for i in range(1, 22)]
    test_data = pd.read_csv('test_FD001.txt', sep='\\s+', header=None, names=column_names)
    print(f"✅ Step 2: Test data loaded. Ready to stream {len(test_data)} readings.")
except FileNotFoundError:
    print("❌ ERROR: The file 'test_FD001.txt' was not found.")
    print("➡ FIX: Make sure the test data file is in the same folder as this script.")
    exit() # Stop the script if the file is missing.

# --- Step 3: Start Sending Live Data! ---
print(f"\n--- Now sending live data to the '{KAFKA_TOPIC}' channel. Press Ctrl+C to stop. ---")

# We go through the test data row by row.
for index, row in test_data.iterrows():
    # Convert the row of data into a dictionary format.
    message = row.to_dict()

    # Print a status to the screen so we can see it working.
    print(f"Reporting: Engine ID {int(message['engine_id'])}, Cycle {int(message['cycle'])}")

    # Send the message to our Kafka topic.
    producer.send(KAFKA_TOPIC, value=message)

    # Wait for one second to make it feel like a real-time stream.
    time.sleep(1)

# Ensure all messages are sent before the script closes.
producer.flush()
print("\n--- Live Reporter has finished sending all data. ---")