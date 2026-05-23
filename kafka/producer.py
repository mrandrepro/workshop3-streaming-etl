"""
Kafka Producer - Workshop 3
Streams World Happiness records to topic: happiness-predictions
"""
import json
import time
import pandas as pd
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "happiness-predictions")
DATA_PATH = os.path.join(os.path.dirname(__file__), "../data/processed/unified.csv")
DELAY_SECONDS = 0.5


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def stream_records(producer, df):
    print(f"[Producer] Streaming {len(df)} records to topic '{KAFKA_TOPIC}'...")
    for _, row in df.iterrows():
        event = {
            "country": str(row.get("country", "")),
            "year": int(row.get("year", 0)),
            "gdp": float(row.get("gdp", 0.0)),
            "family": float(row.get("family", 0.0)),
            "health": float(row.get("health", 0.0)),
            "freedom": float(row.get("freedom", 0.0)),
            "generosity": float(row.get("generosity", 0.0)),
            "corruption": float(row.get("corruption", 0.0)),
            "actual_happiness_score": float(row.get("happiness_score", 0.0)),
        }
        producer.send(KAFKA_TOPIC, value=event)
        print(f"  -> Sent: {event['country']} ({event['year']})")
        time.sleep(DELAY_SECONDS)
    producer.flush()
    print("[Producer] All records sent.")


if __name__ == "__main__":
    df = pd.read_csv(DATA_PATH)
    producer = create_producer()
    try:
        stream_records(producer, df)
    finally:
        producer.close()
