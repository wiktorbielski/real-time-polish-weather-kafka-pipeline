-- 1. Hourly temperature trend per city (great for line charts)
CREATE VIEW `weather_dataset.hourly_temp_trend` AS
SELECT
  city,
  TIMESTAMP_TRUNC(api_timestamp, HOUR) AS hour,
  AVG(temperature) AS avg_temp,
  MIN(temperature) AS min_temp,
  MAX(temperature) AS max_temp
FROM `weather_dataset.raw_weather`
GROUP BY city, hour
ORDER BY city, hour;

-- 2. Weather condition distribution (great for pie/bar charts) 
CREATE VIEW `weather_dataset.weather_condition_summary` AS
SELECT
  city,
  weather_main,
  weather_description,
  COUNT(*) AS occurrence_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY city), 2) AS percentage
FROM `weather_dataset.raw_weather`
GROUP BY city, weather_main, weather_description;

-- 3. City rankings by temperature (great for scorecards/tables)
CREATE VIEW `weather_dataset.city_temperature_ranking` AS
SELECT
  city,
  ROUND(AVG(temperature), 2) AS avg_temp,
  ROUND(MIN(temperature), 2) AS min_temp,
  ROUND(MAX(temperature), 2) AS max_temp,
  ROUND(MAX(temperature) - MIN(temperature), 2) AS temp_range,
  RANK() OVER (ORDER BY AVG(temperature) DESC) AS rank_hottest
FROM `weather_dataset.raw_weather`
GROUP BY city;

-- 4. Wind speed analysis per city (good for bar charts)
CREATE VIEW `weather_dataset.wind_analysis` AS
SELECT
  city,
  ROUND(AVG(wind_speed), 2) AS avg_wind_speed,
  ROUND(MAX(wind_speed), 2) AS max_wind_speed,
  CASE
    WHEN AVG(wind_speed) < 3  THEN 'Calm'
    WHEN AVG(wind_speed) < 8  THEN 'Light Breeze'
    WHEN AVG(wind_speed) < 14 THEN 'Moderate Wind'
    ELSE 'Strong Wind'
  END AS wind_category
FROM `weather_dataset.raw_weather`
GROUP BY city;

-- 5. Humidity vs Temperature correlation (great for scatter plots)
CREATE VIEW `weather_dataset.humidity_temp_correlation` AS
SELECT
  city,
  TIMESTAMP_TRUNC(api_timestamp, HOUR) AS hour,
  ROUND(AVG(temperature), 2) AS avg_temp,
  ROUND(AVG(humidity), 2) AS avg_humidity,
  CASE
    WHEN AVG(humidity) >= 80 THEN 'High Humidity'
    WHEN AVG(humidity) >= 50 THEN 'Moderate Humidity'
    ELSE 'Low Humidity'
  END AS humidity_category
FROM `weather_dataset.raw_weather`
GROUP BY city, hour;

