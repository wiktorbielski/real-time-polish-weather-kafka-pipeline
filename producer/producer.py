import requests
import json
import time
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv('OPENWEATHER_API_KEY')
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
TOPIC = os.getenv('KAFKA_TOPIC')

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

cities = ["Warszawa", "Krakow", "Lodz", "Wroclaw", "Poznan", "Gdansk", "Szczecin", "Bydgoszcz", "Lublin", "Bialystok"]

while True:
    for city in cities:
        try:
            url = f"https://api.openweathermap.org/data/2.5/weather?q={city},PL&appid={API_KEY}&units=metric"
            response = requests.get(url)
            data = response.json()
            if response.status_code == 200:
                weather_data = {
                    'city': city,
                    'temperature': data['main']['temp'],
                    'humidity': data['main']['humidity'],
                    'weather_main': data['weather'][0]['main'],
                    'weather_description': data['weather'][0]['description'],
                    'wind_speed': data['wind']['speed'],
                    'api_timestamp': datetime.fromtimestamp(data['dt']).isoformat(),
                    'ingestion_timestamp': datetime.now().isoformat()
                }
                producer.send(TOPIC, value=weather_data)
                print(f"Wysłano dane dla {city}: {weather_data}")
            else:
                print(f"Błąd dla {city}: {data}")
        except Exception as e:
            print(f"Wyjątek dla {city}: {e}")
    time.sleep(60) 