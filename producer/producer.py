import os
import json
import time
from datetime import datetime
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# Configuration
API_KEY = os.getenv('OPENWEATHER_API_KEY')
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
TOPIC = os.getenv('KAFKA_TOPIC')

# Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),

    # Reliability: Wait for all replicas to acknowledge to prevent data loss 
    # if a broker crashes immediately after receiving a message.
    acks='all',

    # Ordering & Retries: Allows up to 5 concurrent requests while maintaining 
    # message order during retries.
    retries=10,
    max_in_flight_requests_per_connection=5,
    
    # Network: Timeout for requests and periodic metadata refresh (5 mins) 
    # to handle broker restarts gracefully.
    request_timeout_ms=45000,
    metadata_max_age_ms=300000
)

# Operational Constants
CITIES = [
    "Warsaw", "Krakow", "Lodz", "Wroclaw", "Poznan",
    "Gdansk", "Szczecin", "Bydgoszcz", "Lublin", "Bialystok"
]

# Respect OpenWeatherMap free tier limits (60 req/min)
DELAY_BETWEEN_CITIES = 0.6 
DELAY_BETWEEN_CYCLES = 60 

print(f"[START] Weather producer listening on topic: '{TOPIC}'")

while True:
    for city in CITIES:
        try:
            params = {
                'q': f"{city},PL",
                'appid': API_KEY,
                'units': 'metric'
            }
            
            # Prevents hanging indefinitely on slow network responses
            response = requests.get(
                "https://api.openweathermap.org/data/2.5/weather",
                params=params,
                timeout=10 
            )

            data = response.json()

            if response.status_code == 200:
                weather_payload = {
                    'city': city,
                    'temperature': data['main'].get('temp'),
                    'humidity': data['main'].get('humidity'),
                    'weather_main': data['weather'][0].get('main') if data.get('weather') else None,
                    'weather_description': data['weather'][0].get('description') if data.get('weather') else None,
                    'wind_speed': data.get('wind', {}).get('speed'),

                    # Original measurement time from the weather station (Unix UTC)
                    'api_timestamp': datetime.fromtimestamp(data['dt']).isoformat() if data.get('dt') else None,

                    # Local time when this specific script processed the data
                    'ingestion_timestamp': datetime.now().isoformat()
                }

                # Send is async; message is added to an internal buffer
                producer.send(TOPIC, value=weather_payload)
                print(f"[SENT] city={city} | temp={weather_payload['temperature']}C")

            else:
                print(f"[ERROR] Failed to fetch data for {city}: {data.get('message', 'Unknown error')}")

        except requests.exceptions.Timeout:
            print(f"[TIMEOUT] No response from API for city: {city}")
        except Exception as e:
            print(f"[ERROR] Exception for {city}: {type(e).__name__}: {e}")

        time.sleep(DELAY_BETWEEN_CITIES)

    # Blocks until all buffered messages are acknowledged by the broker
    # to ensure no data is lost between cycles.
    producer.flush()
    print(f"[CYCLE] Complete. Waiting {DELAY_BETWEEN_CYCLES} seconds...")
    time.sleep(DELAY_BETWEEN_CYCLES)