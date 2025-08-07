from flask import Flask, request, jsonify
from kafka import KafkaProducer, KafkaConsumer
import json
import time
import requests
import threading
from threading import Lock

app = Flask(__name__)

# Kafka configuration
KAFKA_BROKER = 'kafka:9092'  # Kafka broker address (inside Docker network)
KAFKA_TOPIC = 'weather-api'

# Pinot configuration
PINOT_INGEST_URL = 'http://localhost:9000/ingestion/ingest'  # Replace with your Pinot ingestion endpoint
PINOT_TABLE_NAME = 'weather_schema'  # Replace with your table name

# Kafka producer setup
def create_kafka_producer():
    retries = 5  # Number of retries before failing
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Connected to Kafka successfully")
            return producer
        except Exception as e:
            print(f"Error connecting to Kafka: {e}")
            retries -= 1
            time.sleep(5)  # Wait for 5 seconds before retrying
    raise Exception("Failed to connect to Kafka after several attempts")

# Create producer with retries
producer = create_kafka_producer()

# Global list to store consumed messages
consumed_messages = []
messages_lock = Lock()

# Weather API configuration (Open-Meteo API)
WEATHER_API_URL = 'https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m'

@app.route('/fetch-weather', methods=['GET'])
def fetch_weather():
    """
    Fetch weather data from Open-Meteo API and produce to Kafka.
    """
    try:
        # Fetch weather data from the Open-Meteo API
        response = requests.get(WEATHER_API_URL)
        response.raise_for_status()  # Raise error for non-200 status codes

        weather_data = response.json()
        # Produce weather data to Kafka
        producer.send(KAFKA_TOPIC, value=weather_data)
        producer.flush()

        return jsonify({
            'message': 'Weather data sent to Kafka successfully!',
            'data': weather_data
        }), 200
    except requests.exceptions.RequestException as e:
        return jsonify({'error': f"Request failed: {str(e)}"}), 500

@app.route('/get-weather-data', methods=['GET'])
def get_weather_data():
    """
    Retrieve all consumed weather data.
    """
    with messages_lock:  # Ensure thread-safe access
        if not consumed_messages:
            return jsonify({'message': 'No data available. Please fetch weather first.'}), 404
        return jsonify({'messages': consumed_messages}), 200

def consume_weather_data():
    """
    Consume weather data from Kafka topic and store in a global list.
    Push data to Pinot after consuming.
    """
    global consumed_messages
    try:
        # Create Kafka consumer
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='weather-consumer-group',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        print(f"Connected to Kafka topic: {KAFKA_TOPIC}")
        print("Listening for messages...")

        for message in consumer:
            print(f"Consumed message: {message.value}")
            with messages_lock:  # Ensure thread-safe access
                consumed_messages.append(message.value)
            
            # Push the message to Pinot
            push_to_pinot(message.value)

    except Exception as e:
        print(f"Error consuming data from Kafka: {e}")


def push_to_pinot(weather_data):
    """
    Push the consumed weather data to Pinot.
    """
    # Prepare data for Pinot ingestion (format needs to match Pinot schema)
    pinot_data = [{
        "time": weather_data["current"]["time"],  # Example field, adjust based on your schema
        "temperature_2m": weather_data["current"]["temperature_2m"],
        "wind_speed_10m": weather_data["current"]["wind_speed_10m"],
        "latitude": weather_data["latitude"],
        "longitude": weather_data["longitude"],
        "elevation": weather_data["elevation"],
        "timezone": weather_data["timezone"],
        "timezone_abbreviation": weather_data["timezone_abbreviation"],
        "utc_offset_seconds": weather_data["utc_offset_seconds"]
    }]
    
    headers = {
        'Content-Type': 'application/json'
    }
    
    # Make a POST request to Pinot ingestion API
    try:
        payload = {
            "tableName": PINOT_TABLE_NAME,  # Specify the table name here
            "records": pinot_data
        }
        response = requests.post(
            PINOT_INGEST_URL,
            json=payload,
            headers=headers
        )
        if response.status_code == 200:
            print("Data successfully pushed to Pinot.")
        else:
            print(f"Failed to push data to Pinot: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error pushing data to Pinot: {e}")


if __name__ == '__main__':
    # Start the consumer in a separate thread
    consumer_thread = threading.Thread(target=consume_weather_data)
    consumer_thread.daemon = True  # Ensures the thread exits when the main program stops
    consumer_thread.start()

    # Run the Flask app
    app.run(debug=True, host='0.0.0.0', port=5001)  # Change port to 5001 to avoid conflict with other services
