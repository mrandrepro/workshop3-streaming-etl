-- ============================================================
-- Workshop 3 - Streaming ETL with Apache Kafka and ML
-- Database Schema
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_happiness_events (
    id              SERIAL PRIMARY KEY,
    country         VARCHAR(100),
    year            INTEGER,
    gdp             FLOAT,
    family          FLOAT,
    health          FLOAT,
    freedom         FLOAT,
    generosity      FLOAT,
    corruption      FLOAT,
    actual_happiness_score FLOAT,
    raw_payload     JSONB,
    status          VARCHAR(30) DEFAULT 'VALID',
    received_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_country (
    country_id      SERIAL PRIMARY KEY,
    country_name    VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id         SERIAL PRIMARY KEY,
    year            INTEGER,
    month           INTEGER,
    day             INTEGER,
    full_date       DATE
);

CREATE TABLE IF NOT EXISTS dim_raw_event (
    raw_event_id    INTEGER PRIMARY KEY REFERENCES raw_happiness_events(id),
    status          VARCHAR(30),
    received_at     TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id        SERIAL PRIMARY KEY,
    raw_event_id         INTEGER REFERENCES raw_happiness_events(id),
    country_id           INTEGER REFERENCES dim_country(country_id),
    date_id              INTEGER REFERENCES dim_date(date_id),
    actual_score         FLOAT,
    predicted_score      FLOAT,
    prediction_error     FLOAT,
    prediction_timestamp TIMESTAMP DEFAULT NOW()
);
