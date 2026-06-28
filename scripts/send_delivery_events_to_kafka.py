from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

import pandas as pd
from confluent_kafka import Producer


RAW_EVENTS_PATH = Path("data/raw/delivery_events.csv")

KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC = "delivery_events"


def to_jsonable(value: Any) -> Any:
    """
    Преобразует значения pandas/numpy в обычные Python-типы,
    чтобы их можно было сериализовать в JSON.
    """
    if pd.isna(value):
        return None

    if hasattr(value, "item"):
        return value.item()

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    return value


def row_to_event(row: pd.Series) -> dict[str, Any]:
    event = {
        "event_id": to_jsonable(row["event_id"]),
        "event_time": pd.Timestamp(row["event_time"]).isoformat(),
        "order_id": to_jsonable(row["order_id"]),
        "courier_id": to_jsonable(row["courier_id"]),
        "route_id": to_jsonable(row["route_id"]),
        "event_type": to_jsonable(row["event_type"]),
        "lat": to_jsonable(row["lat"]),
        "lon": to_jsonable(row["lon"]),
    }

    return event


def delivery_report(err, msg) -> None:
    """
    Callback, который Kafka вызывает после попытки отправки сообщения.
    """
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered to topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Сколько событий отправить. Если не указано — отправить все.",
    )
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=0,
        help="Пауза между отправками событий в миллисекундах.",
    )
    args = parser.parse_args()

    if not RAW_EVENTS_PATH.exists():
        raise FileNotFoundError(
            f"File not found: {RAW_EVENTS_PATH}. "
            "Сначала запусти генератор данных."
        )

    events_df = pd.read_csv(RAW_EVENTS_PATH, parse_dates=["event_time"])

    if args.limit is not None:
        events_df = events_df.head(args.limit)

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        }
    )

    print(f"Sending {len(events_df)} events to Kafka topic '{TOPIC}'...")

    sent_count = 0

    for _, row in events_df.iterrows():
        event = row_to_event(row)

        key = str(event["route_id"])
        value = json.dumps(event, ensure_ascii=False).encode("utf-8")

        producer.produce(
            topic=TOPIC,
            key=key.encode("utf-8"),
            value=value,
            callback=delivery_report,
        )

        producer.poll(0)
        sent_count += 1

        if args.sleep_ms > 0:
            time.sleep(args.sleep_ms / 1000)

    producer.flush()

    print(f"Finished. Sent events: {sent_count}")


if __name__ == "__main__":
    main()
