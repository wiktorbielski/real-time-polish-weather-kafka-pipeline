CREATE TABLE `weather_dataset.raw_weather`
(
  city STRING
    OPTIONS(description="Name of the city where the weather measurement was recorded"),

  temperature FLOAT64
    OPTIONS(description="Air temperature in degrees Celsius"),

  humidity INT64
    OPTIONS(description="Relative humidity expressed as a percentage (0–100)"),

  weather_main STRING
    OPTIONS(description="Primary weather condition (e.g., Rain, Clouds, Clear)"),

  weather_description STRING
    OPTIONS(description="Detailed weather condition description from the API"),

  wind_speed FLOAT64
    OPTIONS(description="Wind speed measured in meters per second"),

  api_timestamp TIMESTAMP
    OPTIONS(description="Timestamp provided by the weather API indicating when the data was generated"),

  ingestion_timestamp TIMESTAMP
    OPTIONS(description="Timestamp when the record was ingested into the raw layer"),

  processing_timestamp TIMESTAMP
    OPTIONS(description="Timestamp when the record was processed within the data pipeline")
)
OPTIONS(
  description="Raw weather data ingested from an external API. This table represents the landing layer and serves as the source for downstream transformations and analytics."
);