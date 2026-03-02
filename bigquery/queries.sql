CREATE TABLE `weather_dataset.raw_weather` (
  city STRING,
  temperature FLOAT64,
  humidity INT64,
  weather_main STRING,
  weather_description STRING,
  wind_speed FLOAT64,
  api_timestamp TIMESTAMP,
  ingestion_timestamp TIMESTAMP,
  processing_timestamp TIMESTAMP
);
