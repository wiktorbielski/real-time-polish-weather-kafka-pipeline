import json
import os
from datetime import datetime, timezone
from kafka import KafkaConsumer
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

# Configuration & Auth
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
TOPIC = os.getenv('KAFKA_TOPIC')
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCP_SERVICE_ACCOUNT_KEY_PATH = os.getenv('GCP_SERVICE_ACCOUNT_KEY_PATH')

# Global GCP auth setup: prevents passing credentials to every client manually
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GCP_SERVICE_ACCOUNT_KEY_PATH

client = bigquery.Client(project=GCP_PROJECT_ID)
table_id = f"{GCP_PROJECT_ID}.weather_dataset.raw_weather"

# Batching reduces API overhead and cost compared to individual row inserts
BATCH_SIZE_THRESHOLD = 10

REQUIRED_FIELDS = [
    'city', 'temperature', 'humidity', 'weather_main', 
    'weather_description', 'wind_speed', 'api_timestamp', 'ingestion_timestamp'
]

# Kafka Consumer Setup
# earliest: starts from beginning if no previous offset exists
# group_id: tracks progress independently from other consumers
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='weather-processing-group'
)

def validate_record(record: dict) -> bool:
    """Detects schema drift or missing data before processing."""
    
    # Critical requirement: Weather data without temperature is analytically useless
    if record.get('temperature') is None:
        print(f"[SKIP] Missing temperature for: {record.get('city', 'UNKNOWN')}")
        return False

    # Partial data is better than no data: fill missing nullable fields to avoid crashes
    missing = [f for f in REQUIRED_FIELDS if f not in record]
    if missing:
        print(f"[WARN] {record.get('city')} missing fields: {missing}. Filling with None.")
        for field in missing:
            record[field] = None

    return True

def upload_batch(batch: list) -> bool:
    """Performs a streaming insert to BigQuery."""
    print(f"[UPLOAD] Sending {len(batch)} records...")
    try:
        # insert_rows_json returns a list of error objects (not an exception on row-level fail)
        errors = client.insert_rows_json(table_id, batch)
        if not errors:
            print(f"[OK] Batch loaded to {table_id}")
            return True
        
        print(f"[ERROR] BigQuery insert errors: {errors}")
        return False
    except Exception as e:
        print(f"[ERROR] Connection/Auth exception: {type(e).__name__}: {e}")
        return False

# Main Execution
print(f"[START] Consumer listening on topic: '{TOPIC}'")
data_batch = []

try:
    for message in consumer:
        try:
            record = message.value
            if not validate_record(record):
                continue

            # End-to-end latency tracking: combines with API and Ingestion timestamps
            record['processing_timestamp'] = datetime.now(timezone.utc).isoformat()
            data_batch.append(record)
            
            print(f"[RECEIVED] {record.get('city')} | Buffer: {len(data_batch)}/{BATCH_SIZE_THRESHOLD}")

            if len(data_batch) >= BATCH_SIZE_THRESHOLD:
                if upload_batch(data_batch):
                    data_batch = [] # Only clear on success to prevent data loss on API failure
                else:
                    print(f"[RETRY] Retaining {len(data_batch)} records for next attempt.")

        except Exception as e:
            print(f"[ERROR] Message processing failed: {e}")
            continue

except KeyboardInterrupt:
    print("\n[STOP] User interrupted.")

finally:
    # Shutdown safety: flush remaining records even if BATCH_SIZE wasn't met
    if data_batch:
        print(f"[FLUSH] Saving {len(data_batch)} remaining records...")
        upload_batch(data_batch)

    consumer.close()
    print("[STOP] Consumer closed.")