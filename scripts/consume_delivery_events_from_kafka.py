from __future__ import annotations

import argparse
import json

from confluent_kafka import Consumer


KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC = "delivery_events"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--max-messages",
        type=int,
        default=10,
        help="Сколько сообщений прочитать.",
    )
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "ozon-delivery-events-debug-consumer",
            "auto.offset.reset": "earliest",
        }
    )

    consumer.subscribe([TOPIC])

    print(f"Reading up to {args.max_messages} messages from topic '{TOPIC}'...")

    read_count = 0

    try:
        while read_count < args.max_messages:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print("ERROR:", msg.error())
                continue

            event = json.loads(msg.value().decode("utf-8"))

            print(
                {
                    "event_id": event["event_id"],
                    "event_time": event["event_time"],
                    "order_id": event["order_id"],
                    "route_id": event["route_id"],
                    "event_type": event["event_type"],
                }
            )

            read_count += 1

    finally:
        consumer.close()

    print(f"Finished. Read messages: {read_count}")


if __name__ == "__main__":
    main()
