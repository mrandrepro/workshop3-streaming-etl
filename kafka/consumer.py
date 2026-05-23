"""
Kafka Consumer - Workshop 3
Receives events, validates, runs ML inference, stores results in PostgreSQL.
"""
import json
import joblib
import numpy as np
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "happiness-predictions")
MODEL_PATH = os.path.join(os.path.dirname(__file__), "../models/model.pkl")

FEATURE_ORDER = ["gdp", "family", "health", "freedom", "generosity", "corruption"]
REQUIRED_FIELDS = FEATURE_ORDER + ["country", "year", "actual_happiness_score"]


def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="happiness_db",
        user="etl_user",
        password="etl_pass",
    )


def store_raw_event(conn, event, status):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO raw_happiness_events
               (country, year, gdp, family, health, freedom,
                generosity, corruption, actual_happiness_score, raw_payload, status)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
            (event.get("country"), event.get("year"), event.get("gdp"),
             event.get("family"), event.get("health"), event.get("freedom"),
             event.get("generosity"), event.get("corruption"),
             event.get("actual_happiness_score"), json.dumps(event), status),
        )
        raw_id = cur.fetchone()[0]
    conn.commit()
    return raw_id


def get_or_create_country(conn, country_name):
    with conn.cursor() as cur:
        cur.execute("SELECT country_id FROM dim_country WHERE country_name = %s", (country_name,))
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute("INSERT INTO dim_country (country_name) VALUES (%s) RETURNING country_id", (country_name,))
        cid = cur.fetchone()[0]
    conn.commit()
    return cid


def get_or_create_date(conn, year):
    with conn.cursor() as cur:
        cur.execute("SELECT date_id FROM dim_date WHERE year = %s AND month IS NULL", (year,))
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute("INSERT INTO dim_date (year) VALUES (%s) RETURNING date_id", (year,))
        did = cur.fetchone()[0]
    conn.commit()
    return did


def store_prediction(conn, raw_event_id, country_id, date_id, actual, predicted):
    error = predicted - actual
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO fact_predictions
               (raw_event_id, country_id, date_id, actual_score, predicted_score, prediction_error)
               VALUES (%s,%s,%s,%s,%s,%s)""",
            (raw_event_id, country_id, date_id, actual, predicted, error),
        )
    conn.commit()


def validate_event(event):
    for field in REQUIRED_FIELDS:
        if field not in event:
            return "INVALID_SCHEMA"
    for field in FEATURE_ORDER + ["actual_happiness_score"]:
        try:
            val = float(event[field])
            if val < 0:
                return "INVALID_VALUES"
        except (TypeError, ValueError):
            return "INVALID_VALUES"
    return "VALID"


def main():
    print("[Consumer] Loading model...")
    model = joblib.load(MODEL_PATH)

    print("[Consumer] Connecting to Kafka...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="happiness-consumer-group",
    )

    print(f"[Consumer] Listening on topic '{KAFKA_TOPIC}'...")
    conn = get_connection()

    for message in consumer:
        event = message.value
        print(f"\n[Consumer] Received: {event.get('country')} ({event.get('year')})")

        status = validate_event(event)
        raw_id = store_raw_event(conn, event, status)
        print(f"  Raw stored (id={raw_id}, status={status})")

        if status != "VALID":
            print(f"  Skipping prediction -- {status}")
            continue

        try:
            features = np.array([[float(event[f]) for f in FEATURE_ORDER]])
            predicted = float(model.predict(features)[0])
            actual = float(event["actual_happiness_score"])
            country_id = get_or_create_country(conn, event["country"])
            date_id = get_or_create_date(conn, event["year"])
            store_prediction(conn, raw_id, country_id, date_id, actual, predicted)
            print(f"  Predicted: {predicted:.4f} | Actual: {actual:.4f} | Error: {predicted - actual:.4f}")
        except Exception as e:
            print(f"  PREDICTION_ERROR: {e}")
            with conn.cursor() as cur:
                cur.execute("UPDATE raw_happiness_events SET status='PREDICTION_ERROR' WHERE id=%s", (raw_id,))
            conn.commit()


if __name__ == "__main__":
    main()
