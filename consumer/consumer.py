import json
from kafka import KafkaConsumer
from google.cloud import bigquery
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
TOPIC = os.getenv('KAFKA_TOPIC')
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCP_SERVICE_ACCOUNT_KEY_PATH = os.getenv('GCP_SERVICE_ACCOUNT_KEY_PATH')

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GCP_SERVICE_ACCOUNT_KEY_PATH
client = bigquery.Client(project=GCP_PROJECT_ID)
table_id = f"{GCP_PROJECT_ID}.weather_dataset.raw_weather"

consumer = KafkaConsumer(TOPIC,
                         bootstrap_servers=BOOTSTRAP_SERVERS,
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_commit=True)

batch = []
batch_size = 10  # Ładuj co 10 wiadomości

for message in consumer:
    data = message.value
    # Transform
    data['processing_timestamp'] = datetime.now().isoformat()
    # Walidacja (prosta)
    if data.get('temperature') is not None:
        batch.append(data)
    
    if len(batch) >= batch_size:
        errors = client.insert_rows_json(table_id, batch)
        if not errors:
            print("Załadowano batch do BigQuery")
        else:
            print(f"Błędy: {errors}")
        batch = []