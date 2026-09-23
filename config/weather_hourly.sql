CREATE TABLE IF NOT EXISTS weather_hourly (
    hour TIMESTAMP NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    avg_temperature DOUBLE PRECISION,
    min_temperature DOUBLE PRECISION,
    max_temperature DOUBLE PRECISION,
    avg_humidity DOUBLE PRECISION,
    avg_wind_speed DOUBLE PRECISION,
    max_wind_speed DOUBLE PRECISION,
    observation_count BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (hour, latitude, longitude)
);
