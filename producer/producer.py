import requests
import json
import time
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()

# API
API_KEY = os.getenv('OPENWEATHER_API_KEY')

# Kafka
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
TOPIC = os.getenv('KAFKA_TOPIC')

# acks='all' ensures the broker confirms the message was written to all in-sync replicas
# before acknowledging — prevents data loss if a broker crashes immediately after receiving.
# retries=10 with max_in_flight_requests_per_connection=5 allows parallel sends
# while still retrying failed ones without reordering messages.
# metadata_max_age_ms=300000 forces the producer to refresh broker metadata every 5 minutes,
# which helps recover from broker restarts without restarting the producer itself.
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=10,
    request_timeout_ms=45000,
    metadata_max_age_ms=300000,
    max_in_flight_requests_per_connection=5
)

CITIES = [
    "Warsaw", "Krakow", "Lodz", "Wroclaw", "Poznan",
    "Gdansk", "Szczecin", "Bydgoszcz", "Lublin", "Bialystok"
]

DELAY_BETWEEN_CITIES = 0.6   # seconds — avoids hitting OpenWeatherMap rate limits (60 req/min on free tier)
DELAY_BETWEEN_CYCLES = 60    # seconds — controls how frequently weather data is refreshed

print(f"[START] Weather producer listening on topic: '{TOPIC}'")

while True:
    for city in CITIES:
        try:
            # 'units=metric' returns temperature in Celsius.
            # Passing params as a dict lets requests handle URL encoding safely.
            params = {
                'q': f"{city},PL",
                'appid': API_KEY,
                'units': 'metric'
            }
            response = requests.get(
                "https://api.openweathermap.org/data/2.5/weather",
                params=params,
                timeout=10  # prevents the producer from hanging indefinitely on slow API responses
            )

            data = response.json()

            if response.status_code == 200:
                # .get() is used instead of direct access to handle partial API responses gracefully —
                # some fields (e.g. wind) may be missing for certain locations or weather conditions.
                weather_payload = {
                    'city': city,
                    'temperature': data['main'].get('temp'),
                    'humidity': data['main'].get('humidity'),
                    'weather_main': data['weather'][0].get('main') if data.get('weather') else None,
                    'weather_description': data['weather'][0].get('description') if data.get('weather') else None,
                    'wind_speed': data.get('wind', {}).get('speed'),
                    # api_timestamp is the time the weather was measured by the station (Unix UTC from API).
                    # ingestion_timestamp is when this producer collected and sent the data.
                    'api_timestamp': datetime.fromtimestamp(data['dt']).isoformat() if data.get('dt') else None,
                    'ingestion_timestamp': datetime.now().isoformat()
                }

                # producer.send() is asynchronous — it queues the message internally.
                # flush() at the end of each cycle ensures all queued messages are delivered.
                producer.send(TOPIC, value=weather_payload)
                print(f"[SENT] city={city} | temp={weather_payload['temperature']}C")

            else:
                print(f"[ERROR] Failed to fetch data for {city}: {data.get('message', 'Unknown error')}")

        except requests.exceptions.Timeout:
            print(f"[TIMEOUT] No response from API for city: {city}")
        except Exception as e:
            print(f"[ERROR] Exception for {city}: {type(e).__name__}: {e}")

        time.sleep(DELAY_BETWEEN_CITIES)

    # flush() blocks until all buffered messages are acknowledged by the broker.
    # Without this, messages queued at the end of a cycle could be lost if the process exits.
    producer.flush()
    print(f"[CYCLE] Complete. Waiting {DELAY_BETWEEN_CYCLES} seconds...")
    time.sleep(DELAY_BETWEEN_CYCLES)