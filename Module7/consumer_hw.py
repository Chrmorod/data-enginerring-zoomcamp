import json

from kafka import KafkaConsumer


TOPIC = "green-trips"
BOOTSTRAP_SERVERS = "localhost:9092"


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="hw-consumer",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        consumer_timeout_ms=10000,
    )

    count = 0
    total = 0

    for message in consumer:
        total += 1
        trip = message.value

        trip_distance = trip.get("trip_distance")
        if trip_distance is not None and float(trip_distance) > 5.0:
            count += 1

    print(f"Total messages read: {total}")
    print(f"Trips with trip_distance > 5.0: {count}")


if __name__ == "__main__":
    main()