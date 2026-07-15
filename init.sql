CREATE TABLE IF NOT EXISTS meteo (
    "Datetime"    TIMESTAMPTZ PRIMARY KEY,
    temp_out    FLOAT,
    dew_out     FLOAT,
    winddir     FLOAT,
    qnh         FLOAT,
    windspeed   FLOAT,
    hum_out     FLOAT,
    is_forecast  BOOLEAN
);

CREATE TABLE IF NOT EXISTS meteo_prediction (
    "Datetime"    TIMESTAMPTZ,
    model_name  VARCHAR,
    corr_diff   FLOAT,
    corr_sum    FLOAT,
    from_forecast  BOOLEAN,
    PRIMARY KEY ("Datetime", model_name),
    FOREIGN KEY ("Datetime") REFERENCES meteo("Datetime")
);

CREATE TABLE IF NOT EXISTS meteo_prediction_history (
    id SERIAL PRIMARY KEY,
    forecast_at    TIMESTAMPTZ,
    "Datetime"    TIMESTAMPTZ,
    model_name  VARCHAR,
    corr_diff   FLOAT,
    corr_sum    FLOAT,
    temp_out    FLOAT,
    dew_out     FLOAT,
    winddir     FLOAT,
    qnh         FLOAT,
    windspeed   FLOAT,
    hum_out     FLOAT
);

CREATE INDEX IF NOT EXISTS idx_meteo_datetime
    ON meteo("Datetime" ASC);

CREATE INDEX IF NOT EXISTS idx_prediction_datetime
    ON meteo_prediction("Datetime" ASC);

CREATE INDEX IF NOT EXISTS idx_prediction_history_id
    ON meteo_prediction_history(id ASC);