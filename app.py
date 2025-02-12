import requests
import json
import csv
import os
import datetime
import time
import threading
import pandas as pd
from flask import Flask, jsonify, render_template
from flask_socketio import SocketIO

# Phyphox API URL for location, accelerometer, and gyroscope
PHYPOX_URL = "http://192.168.2.11:8080/get?accX&accY&accZ&gyrX&gyrY&gyrZ&locLat&locLon&locZ"

CSV_FILE = "sensor_data.csv"

# Flask + WebSocket Server
app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, cors_allowed_origins="*")

# Ensure CSV file exists and has headers
def initialize_csv():
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["timestamp", "accX", "accY", "accZ", "gyroX", "gyroY", "gyroZ", "latitude", "longitude", "altitude"])

# Function to continuously fetch Phyphox data and stream via WebSocket
def fetch_and_stream_phyphox_data():
    while True:
        try:
            response = requests.get(PHYPOX_URL)
            response.raise_for_status()
            data = response.json()

            timestamp = datetime.datetime.now().isoformat()
            accX = data.get("buffer", {}).get("accX", {}).get("buffer", [None])[-1]
            accY = data.get("buffer", {}).get("accY", {}).get("buffer", [None])[-1]
            accZ = data.get("buffer", {}).get("accZ", {}).get("buffer", [None])[-1]
            gyroX = data.get("buffer", {}).get("gyrX", {}).get("buffer", [None])[-1]
            gyroY = data.get("buffer", {}).get("gyrY", {}).get("buffer", [None])[-1]
            gyroZ = data.get("buffer", {}).get("gyrZ", {}).get("buffer", [None])[-1]
            latitude = data.get("buffer", {}).get("locLat", {}).get("buffer", [None])[-1]
            longitude = data.get("buffer", {}).get("locLon", {}).get("buffer", [None])[-1]
            altitude = data.get("buffer", {}).get("locZ", {}).get("buffer", [None])[-1]

            # Save to CSV
            with open(CSV_FILE, "a", newline="") as file:
                writer = csv.writer(file)
                writer.writerow([timestamp, accX, accY, accZ, gyroX, gyroY, gyroZ, latitude, longitude, altitude])

            # Send data to WebSocket clients
            socketio.emit("sensor_data", {
                "timestamp": timestamp,
                "acceleration": {"x": accX, "y": accY, "z": accZ},
                "gyroscope": {"x": gyroX, "y": gyroY, "z": gyroZ},
                "location": {"lat": latitude, "lon": longitude, "alt": altitude}
            })

            print(f"Sent: Acc({accX}, {accY}, {accZ}) Gyro({gyroX}, {gyroY}, {gyroZ}) Loc({latitude}, {longitude}, {altitude})")

        except requests.exceptions.RequestException as e:
            print(f"[⚠] Warning: Could not fetch data from Phyphox ({e}). Retrying in 5 seconds...")
            time.sleep(5)  # Wait before retrying

        time.sleep(1)  # Fetch data every 1 second


# Flask Route to Serve Latest Data as JSON
@app.route("/get_latest_data")
def get_latest_data():
    try:
        df = pd.read_csv(CSV_FILE)
        latest_data = df.iloc[-1].to_dict()
        return jsonify(latest_data)
    except Exception as e:
        return jsonify({"error": "No data available", "details": str(e)})
    
# Flask Route to Serve Web Interface
@app.route("/")
def index():
    return render_template("index.html")

# Run Flask and WebSocket in a separate thread
if __name__ == "__main__":
    initialize_csv()
    threading.Thread(target=fetch_and_stream_phyphox_data, daemon=True).start()
    socketio.run(app, debug=False)
