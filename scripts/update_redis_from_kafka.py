from __future__ import annotations

import argparse
import json
import time
from typing import Any

import redis
from confluent_kafka import Consumer


KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC = "delivery_events"

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379


def normalize_event(event: dict[str, Any]) -> dict[str, str]:
    """
    Redis hash удобнее хранить строками.
    Поэтому все значения приводим к строкам.
    """
    return {
        "event_id": str(event.get("event_id")),
        "event_time": str(event.get("event_time")),
        "order_id": str(event.get("order_id")),
        "courier_id": str(event.get("courier_id")),
        "route_id": str(event.get("route_id")),
        "event_type": str(event.get("event_type")),
        "lat": str(event.get("lat")),
        "lon": str(event.get("lon")),
    }


def update_redis_state(r: redis.Redis, event: dict[str, Any]) -> None:
    """
    Обновляет последнее состояние маршрута и курьера в Redis.

    Для каждого события пишем:
    - route:{route_id}:state
    - courier:{courier_id}:state
    - route:{route_id}:events_count
    - courier:{courier_id}:events_count
    """
    route_id = event["route_id"]
    courier_id = event["courier_id"]

    route_state_key = f"route:{route_id}:state"
    courier_state_key = f"courier:{courier_id}:state"

    route_events_count_key = f"route:{route_id}:events_count"
    courier_events_count_key = f"courier:{courier_id}:events_count"

    route_last_event_json_key = f"route:{route_id}:last_event_json"
    courier_last_event_json_key = f"courier:{courier_id}:last_event_json"

    normalized_event = normalize_event(event)
    event_json = json.dumps(event, ensure_ascii=False)

    pipe = r.pipeline()

    pipe.hset(route_state_key, mapping=normalized_event)
    pipe.hset(courier_state_key, mapping=normalized_event)

    pipe.incr(route_events_count_key)
    pipe.incr(courier_events_count_key)

    pipe.set(route_last_event_json_key, event_json)
    pipe.set(courier_last_event_json_key, event_json)

    pipe.execute()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--max-messages",
        type=int,
        default=20,
        help="Сколько сообщений прочитать из Kafka.",
    )
    parser.add_argument(
        "--group-id",
        type=str,
        default="ozon-redis-state-updater",
        help="Kafka consumer group id.",
    )
    parser.add_argument(
        "--idle-timeout-sec",
        type=int,
        default=10,
        help="Через сколько секунд без сообщений завершить consumer.",
    )

    args = parser.parse_args()

    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": args.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )

    consumer.subscribe([TOPIC])

    print(f"Reading topic '{TOPIC}' and updating Redis...")
    print(f"Consumer group: {args.group_id}")
    print(f"Max messages: {args.max_messages}")

    read_count = 0
    last_message_time = time.time()

    try:
        while read_count < args.max_messages:
            msg = consumer.poll(1.0)

            if msg is None:
                if time.time() - last_message_time > args.idle_timeout_sec:
                    print("No messages for a while. Stopping consumer.")
                    break
                continue

            if msg.error():
                print("Kafka error:", msg.error())
                continue

            last_message_time = time.time()

            event = json.loads(msg.value().decode("utf-8"))
            update_redis_state(r, event)

            print(
                {
                    "event_id": event["event_id"],
                    "route_id": event["route_id"],
                    "courier_id": event["courier_id"],
                    "event_type": event["event_type"],
                    "redis_route_key": f"route:{event['route_id']}:state",
                    "redis_courier_key": f"courier:{event['courier_id']}:state",
                }
            )

            read_count += 1

    finally:
        consumer.close()

    print(f"Finished. Processed messages: {read_count}")


if __name__ == "__main__":
    main()
