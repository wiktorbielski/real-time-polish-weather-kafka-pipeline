import json
import os
from datetime import datetime, timezone
from kafka import KafkaConsumer
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

# Kafka
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
TOPIC = os.getenv('KAFKA_TOPIC')

# GCP
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCP_SERVICE_ACCOUNT_KEY_PATH = os.getenv('GCP_SERVICE_ACCOUNT_KEY_PATH')

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GCP_SERVICE_ACCOUNT_KEY_PATH

client = bigquery.Client(project=GCP_PROJECT_ID)
table_id = f"{GCP_PROJECT_ID}.weather_dataset.raw_weather"

BATCH_SIZE_THRESHOLD = 10

REQUIRED_FIELDS = [
    'city', 'temperature', 'humidity',
    'weather_main', 'weather_description',
    'wind_speed', 'api_timestamp', 'ingestion_timestamp'
]

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='weather-processing-group'
)


def validate_record(record: dict) -> bool:
    """
    Checks if record contains required fields and has a valid temperature.
    Missing fields are filled with None instead of rejecting the record.
    """
    if record.get('temperature') is None:
        print(f"[SKIP] Record rejected - missing temperature: {record.get('city', 'UNKNOWN')}")
        return False

    missing = [f for f in REQUIRED_FIELDS if f not in record]
    if missing:
        print(f"[WARN] Record for {record.get('city')} is missing fields: {missing}. Filling with None.")
        for field in missing:
            record[field] = None

    return True


def upload_batch(batch: list) -> bool:
    """Sends a batch of records to BigQuery. Returns True on success."""
    print(f"[UPLOAD] Sending {len(batch)} records to BigQuery...")
    try:
        errors = client.insert_rows_json(table_id, batch)
        if not errors:
            print(f"[OK] Batch successfully loaded to: {table_id}")
            return True
        else:
            print(f"[ERROR] BigQuery insert errors: {errors}")
            return False
    except Exception as e:
        print(f"[ERROR] Exception during BigQuery upload: {type(e).__name__}: {e}")
        return False


print(f"[START] Consumer listening on topic: '{TOPIC}'")

data_batch = []

try:
    for message in consumer:
        try:
            record = message.value

            if not validate_record(record):
                continue

            record['processing_timestamp'] = datetime.now(timezone.utc).isoformat()

            data_batch.append(record)
            print(
                f"[RECEIVED] city={record.get('city')} | "
                f"temp={record.get('temperature')}C | "
                f"buffer={len(data_batch)}/{BATCH_SIZE_THRESHOLD}"
            )

            if len(data_batch) >= BATCH_SIZE_THRESHOLD:
                upload_batch(data_batch)
                data_batch = []

        except Exception as e:
            print(f"[ERROR] Failed to process message: {type(e).__name__}: {e}")
            continue

except KeyboardInterrupt:
    print("\n[STOP] Interrupted by user.")

finally:
    if data_batch:
        print(f"[FLUSH] Saving remaining {len(data_batch)} records from buffer...")
        upload_batch(data_batch)

    consumer.close()
    print("[STOP] Consumer closed.")