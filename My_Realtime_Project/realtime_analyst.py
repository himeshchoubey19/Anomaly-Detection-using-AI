# FILE: realtime_analyst.py (Correctly Indented)

import pandas as pd
import numpy as np
import json
import pickle
from kafka import KafkaConsumer, KafkaProducer # Make sure KafkaProducer is imported
from tensorflow.keras.models import load_model
import warnings

# Suppress a common warning from scikit-learn to keep the output clean
warnings.filterwarnings("ignore", category=UserWarning, module='sklearn')

print("--- Starting Real-Time Analyst ---")

# Load the AI model and the data scaler
model = load_model('engine_rul_model.h5')
scaler = pickle.load(open('data_scaler.pkl', 'rb'))
print("✅ AI Model and Scaler loaded.")

# This dictionary will hold the recent sensor history for each engine
engine_history = {}
WINDOW_SIZE = 50
COLUMNS_THE_SCALER_EXPECTS = scaler.get_feature_names_out()

# Create a producer to send the prediction results
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda data: json.dumps(data).encode('utf-8')
)
PREDICTION_TOPIC = 'prediction-stream'

# Connect to Kafka to listen for live sensor data
consumer = KafkaConsumer(
    'sensor-stream',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda message: json.loads(message.decode('utf-8')),
    auto_offset_reset='latest'
)
print("✅ Connected to Kafka. Listening for live engine data...")

# This is the main loop that processes messages one by one
for message in consumer:
    data = message.value
    engine_id = data['engine_id']

    if engine_id not in engine_history:
        engine_history[engine_id] = []
    engine_history[engine_id].append(data)

    if len(engine_history[engine_id]) > WINDOW_SIZE:
        engine_history[engine_id].pop(0)

    # This block is indented correctly under the 'for' loop
    if len(engine_history[engine_id]) == WINDOW_SIZE:
        # These lines are all indented correctly under the 'if' statement
        window_df = pd.DataFrame(engine_history[engine_id])
        window_for_scaler = window_df[COLUMNS_THE_SCALER_EXPECTS]
        scaled_sensors = scaler.transform(window_for_scaler)
        input_data = np.reshape(scaled_sensors, (1, WINDOW_SIZE, len(COLUMNS_THE_SCALER_EXPECTS)))

        predicted_rul = model.predict(input_data)[0][0]

        result = {'engine_id': int(engine_id), 'predicted_rul': float(predicted_rul)}
        producer.send(PREDICTION_TOPIC, value=result)

        print(f"🔥 Engine {engine_id} -> Predicted RUL: {predicted_rul:.2f} (Published to Kafka)")