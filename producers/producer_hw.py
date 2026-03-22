import json
from time import time

import pandas as pd
from kafka import KafkaProducer


TOPIC = "green-trips"
BOOTSTRAP_SERVERS = "localhost:9092"
PARQUET_FILE = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2015-10.parquet"

COLUMNS = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
    "total_amount",
]


def normalize_record(record: dict) -> dict:
    out = {}

    for key, value in record.items():
        if pd.isna(value):
            out[key] = None
        elif "datetime" in key:
            out[key] = str(value)
        elif key in {"PULocationID", "DOLocationID", "passenger_count"}:
            out[key] = int(value)
        elif key in {"trip_distance", "tip_amount", "total_amount"}:
            out[key] = float(value)
        else:
            out[key] = value

    return out


def main():
    df = pd.read_parquet(PARQUET_FILE, columns=COLUMNS)

    records = df.to_dict(orient="records")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=20,
        retries=5,
    )

    t0 = time()

    for record in records:
        producer.send(TOPIC, value=normalize_record(record))

    producer.flush()

    t1 = time()
    print(f"Sent {len(records)} messages")
    print(f"took {(t1 - t0):.2f} seconds")


if __name__ == "__main__":
    main()