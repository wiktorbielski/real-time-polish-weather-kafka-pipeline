import json
import os
from datetime import datetime, timezone
from kafka import KafkaConsumer
from google.cloud import bigquery
from dotenv import load_dotenv

# Load configuration from .env file
load_dotenv()

# Kafka Settings
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
TOPIC = os.getenv('KAFKA_TOPIC')

# GCP Settings
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCP_SERVICE_ACCOUNT_KEY_PATH = os.getenv('GCP_SERVICE_ACCOUNT_KEY_PATH')

# Configure BigQuery Client
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GCP_SERVICE_ACCOUNT_KEY_PATH
client = bigquery.Client(project=GCP_PROJECT_ID)
table_id = f"{GCP_PROJECT_ID}.weather_dataset.raw_weather"

BATCH_SIZE_THRESHOLD = 10

# Pola wysyłane przez producenta
REQUIRED_FIELDS = [
    'city', 'temperature', 'humidity',
    'weather_main', 'weather_description',
    'wind_speed', 'api_timestamp', 'ingestion_timestamp'c
]

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='weather-processing-group'
)


def validate_record(record: dict) -> bool:
    """Sprawdza czy rekord zawiera wymagane pola i ma poprawną temperaturę."""
    if record.get('temperature') is None:
        print(f"⚠️  Odrzucono rekord bez temperatury: {record.get('city', 'UNKNOWN')}")
        return False

    missing = [f for f in REQUIRED_FIELDS if f not in record]
    if missing:
        print(f"⚠️  Rekord dla {record.get('city')} brakuje pól: {missing}")
        # Nie odrzucamy — uzupełniamy brakujące pola jako None
        for field in missing:
            record[field] = None

    return True


def upload_batch(batch: list) -> bool:
    """Wysyła batch do BigQuery. Zwraca True przy sukcesie."""
    print(f"📤 Wysyłam batch {len(batch)} rekordów do BigQuery...")
    try:
        errors = client.insert_rows_json(table_id, batch)
        if not errors:
            print(f"✅ Batch załadowany pomyślnie do: {table_id}")
            return True
        else:
            print(f"❌ Błędy podczas insertu do BigQuery: {errors}")
            return False
    except Exception as e:
        print(f"❌ Wyjątek podczas uploadu do BigQuery: {type(e).__name__}: {e}")
        return False


print(f"--- Uruchomiono Consumer: nasłuchiwanie na '{TOPIC}' ---")

data_batch = []

try:
    for message in consumer:
        try:
            record = message.value

            # Walidacja i uzupełnienie brakujących pól
            if not validate_record(record):
                continue

            # Transformacja: dodaj timestamp przetwarzania (UTC)
            record['processing_timestamp'] = datetime.now(timezone.utc).isoformat()

            data_batch.append(record)
            print(f"📥 Odebrano rekord: {record.get('city')} | "
                  f"{record.get('temperature')}°C | "
                  f"Bufor: {len(data_batch)}/{BATCH_SIZE_THRESHOLD}")

            # Bulk insert po osiągnięciu progu
            if len(data_batch) >= BATCH_SIZE_THRESHOLD:
                upload_batch(data_batch)
                data_batch = []  # Reset bufora niezależnie od wyniku uploadu

        except Exception as e:
            print(f"❌ Błąd przetwarzania wiadomości: {type(e).__name__}: {e}")
            continue

except KeyboardInterrupt:
    print("\n⏹️  Zatrzymano przez użytkownika.")

finally:
    # Flush pozostałych danych w buforze przed zamknięciem
    if data_batch:
        print(f"💾 Zapisuję pozostałe {len(data_batch)} rekordów z bufora...")
        upload_batch(data_batch)

    consumer.close()
    print("--- Consumer zamknięty ---")