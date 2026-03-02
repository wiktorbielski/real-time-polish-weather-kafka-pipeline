-- 1. The Main Table
EXPORT DATA OPTIONS(
  uri='gs://weather-pipeline-storage/raw_weather/raw_weather_*.csv',
  format='CSV', overwrite=true, header=true) AS
SELECT * FROM `weather-pipeline-488920.weather_dataset.raw_weather`
LIMIT 1000000000;

-- 2. View: City Temperature Ranking
EXPORT DATA OPTIONS(
  uri='gs://weather-pipeline-storage/city_temperature_ranking/city_temp_*.csv',
  format='CSV', overwrite=true, header=true) AS
SELECT * FROM `weather-pipeline-488920.weather_dataset.city_temperature_ranking`
LIMIT 1000000000;

-- 3. View: Hourly Temp Trend
EXPORT DATA OPTIONS(
  uri='gs://weather-pipeline-storage/hourly_temp_trend/hourly_trend_*.csv',
  format='CSV', overwrite=true, header=true) AS
SELECT * FROM `weather-pipeline-488920.weather_dataset.hourly_temp_trend`
LIMIT 1000000000;

-- 4. View: Humidity Temp Correlation
EXPORT DATA OPTIONS(
  uri='gs://weather-pipeline-storage/humidity_temp_correlation/humidity_corr_*.csv',
  format='CSV', overwrite=true, header=true) AS
SELECT * FROM `weather-pipeline-488920.weather_dataset.humidity_temp_correlation`
LIMIT 1000000000;

-- 5. View: Weather Condition Summary
EXPORT DATA OPTIONS(
  uri='gs://weather-pipeline-storage/weather_condition_summary/weather_summary_*.csv',
  format='CSV', overwrite=true, header=true) AS
SELECT * FROM `weather-pipeline-488920.weather_dataset.weather_condition_summary`
LIMIT 1000000000;

-- 6. View: Wind Analysis
EXPORT DATA OPTIONS(
  uri='gs://weather-pipeline-storage/wind_analysis/wind_analysis_*.csv',
  format='CSV', overwrite=true, header=true) AS
SELECT * FROM `weather-pipeline-488920.weather_dataset.wind_analysis`
LIMIT 1000000000;