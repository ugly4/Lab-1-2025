CREATE DATABASE IF NOT EXISTS weather;

CREATE TABLE IF NOT EXISTS weather.weather_hourly (
    city String,
    timestamp DateTime('Europe/Moscow'),
    temperature Float32,
    precipitation Float32,
    windspeed Float32,
    winddirection Float32
) ENGINE = MergeTree
ORDER BY (city, timestamp);

CREATE TABLE IF NOT EXISTS weather.weather_daily (
    city String,
    date Date,
    temp_min Float32,
    temp_max Float32,
    temp_avg Float32,
    total_precipitation Float32,
    max_windspeed Float32
) ENGINE = MergeTree
ORDER BY (city, date);