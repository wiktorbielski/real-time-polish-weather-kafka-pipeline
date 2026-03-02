import requests
import json
import time
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

# Load configuration from .env file
load_dotenv()

API_KEY = os.getenv('OPENWEATHER_API_KEY')
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
TOPIC = os.getenv('KAFKA_TOPIC')

# Initialize Kafka Producer – bardzo dobre ustawienia!
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',                    # pełna trwałość (przy 1 brokerze działa jak acks=1)
    retries=10,                    # ile razy ponowić
    request_timeout_ms=45000,      # 45 sekund
    metadata_max_age_ms=300000,    # odświeżanie metadanych co 5 min
    max_in_flight_requests_per_connection=5   # ← dodane: zapobiega kolejkowaniu
)

# English city names
cities = [
    "Warsaw", "Krakow", "Lodz", "Wroclaw", "Poznan", 
    "Gdansk", "Szczecin", "Bydgoszcz", "Lublin", "Bialystok"
]

print(f"--- Starting Weather Producer on topic: {TOPIC} ---")

while True:
    for city in cities:
        try:
            # Construct API URL + timeout!
            url = f"https://api.openweathermap.org/data/2.5/weather?q={city},PL&appid={API_KEY}&units=metric"
            response = requests.get(url, timeout=10)   # ← ważne!
            data = response.json()
            
            if response.status_code == 200:
                # Bezpieczniejsze pobieranie danych
                weather_payload = {
                    'city': city,
                    'temperature': data['main'].get('temp'),
                    'humidity': data['main'].get('humidity'),
                    'weather_main': data['weather'][0].get('main') if data.get('weather') else None,
                    'weather_description': data['weather'][0].get('description') if data.get('weather') else None,
                    'wind_speed': data.get('wind', {}).get('speed'),
                    'api_timestamp': datetime.fromtimestamp(data['dt']).isoformat() if data.get('dt') else None,
                    'ingestion_timestamp': datetime.now().isoformat()
                }
                
                # Send to Kafka
                producer.send(TOPIC, value=weather_payload)
                print(f"✅ Sent data for {city}: {weather_payload['temperature']}°C")
                
            else:
                print(f"❌ Error fetching data for {city}: {data.get('message', 'Unknown error')}")
                
        except requests.exceptions.Timeout:
            print(f"⏰ Timeout for {city} – API nie odpowiedziało")
        except Exception as e:
            print(f"❌ Exception for {city}: {type(e).__name__}: {e}")
        
        time.sleep(0.6)  # ← mała przerwa między miastami (6 sekund na 10 miast)

    # Zapewniamy, że wszystkie wiadomości zostały wysłane
    producer.flush()
    print("--- Cycle complete. Waiting 60 seconds... ---")
    time.sleep(60)