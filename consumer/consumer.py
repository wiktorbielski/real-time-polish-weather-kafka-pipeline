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

# GOOGLE_APPLICATION_CREDENTIALS is the standard env variable used by all Google Cloud SDKs
# to locate the service account key file for authentication — setting it here avoids
# having to pass credentials explicitly to every GCP client.
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GCP_SERVICE_ACCOUNT_KEY_PATH

client = bigquery.Client(project=GCP_PROJECT_ID)
table_id = f"{GCP_PROJECT_ID}.weather_dataset.raw_weather"

# Number of records to accumulate before sending to BigQuery.
# Batching reduces the number of API calls and improves insert efficiency —
# insert_rows_json has per-request overhead, so sending 10 rows at once
# is significantly cheaper than 10 individual inserts.
BATCH_SIZE_THRESHOLD = 10

# Fields expected in every record produced by the weather producer.
# Used in validate_record() to detect schema drift or producer-side bugs early.
REQUIRED_FIELDS = [
    'city', 'temperature', 'humidity',
    'weather_main', 'weather_description',
    'wind_speed', 'api_timestamp', 'ingestion_timestamp'
]

# auto_offset_reset='earliest' means if this consumer group has no committed offset
# (e.g. first run), it will start reading from the beginning of the topic.
# enable_auto_commit=True automatically commits offsets after each poll,
# so if the consumer restarts, it won't reprocess already-seen messages.
# group_id ensures Kafka tracks this consumer's position independently
# from any other consumers reading the same topic.
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
    # Temperature is treated as a hard requirement — a weather record without
    # a temperature value has no analytical value and should not be stored.
    if record.get('temperature') is None:
        print(f"[SKIP] Record rejected - missing temperature: {record.get('city', 'UNKNOWN')}")
        return False

    # For other missing fields, we fill with None rather than reject the record —
    # partial data is still useful, and BigQuery accepts nullable fields.
    # This also prevents pipeline stalls caused by minor producer-side schema changes.
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
        # insert_rows_json performs a streaming insert — data is available for querying
        # in BigQuery almost immediately, but may take up to 90 minutes to appear
        # in TABLE_DATE_RANGE or exports. It returns a list of errors per row, not an exception.
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

            # processing_timestamp captures when this consumer received and processed the record.
            # Together with api_timestamp and ingestion_timestamp from the producer,
            # it allows measuring end-to-end pipeline latency at each stage.
            record['processing_timestamp'] = datetime.now(timezone.utc).isoformat()

            data_batch.append(record)
            print(
                f"[RECEIVED] city={record.get('city')} | "
                f"temp={record.get('temperature')}C | "
                f"buffer={len(data_batch)}/{BATCH_SIZE_THRESHOLD}"
            )

            if len(data_batch) >= BATCH_SIZE_THRESHOLD:
                success = upload_batch(data_batch)
                # Only clear the buffer on successful upload — if the upload fails,
                # records stay in the buffer and will be retried in the next cycle.
                # This risks duplicates if the insert partially succeeded,
                # but prevents silent data loss on full failure.
                if success:
                    data_batch = []
                else:
                    print(f"[RETRY] Data remains in buffer ({len(data_batch)} records). Retrying in the next cycle.")

        except Exception as e:
            print(f"[ERROR] Failed to process message: {type(e).__name__}: {e}")
            continue

except KeyboardInterrupt:
    print("\n[STOP] Interrupted by user.")

finally:
    # Flush any records that didn't reach BATCH_SIZE_THRESHOLD before shutdown.
    # This runs whether the consumer stopped cleanly or due to an error,
    # ensuring no data is silently dropped at the end of a session.
    if data_batch:
        print(f"[FLUSH] Saving remaining {len(data_batch)} records from buffer...")
        upload_batch(data_batch)

    consumer.close()
    print("[STOP] Consumer closed.")