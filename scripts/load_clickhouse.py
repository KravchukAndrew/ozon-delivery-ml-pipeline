from __future__ import annotations

from pathlib import Path

import pandas as pd
import clickhouse_connect


RAW_DIR = Path("data/raw")


def main() -> None:
    events_path = RAW_DIR / "delivery_events.csv"

    if not events_path.exists():
        raise FileNotFoundError(f"File not found: {events_path}")

    events = pd.read_csv(events_path, parse_dates=["event_time"])

    client = clickhouse_connect.get_client(
        host="127.0.0.1",
        port=8123,
        username="default",
        password="ozon",
        database="ozon",
    )

    client.command("DROP TABLE IF EXISTS delivery_events")

    client.command("""
        CREATE TABLE delivery_events (
            event_id UInt64,
            event_time DateTime,
            order_id UInt64,
            courier_id UInt64,
            route_id UInt64,
            event_type String,
            lat Float64,
            lon Float64
        )
        ENGINE = MergeTree
        ORDER BY (event_time, route_id, order_id, event_id)
    """)

    events = events[
        [
            "event_id",
            "event_time",
            "order_id",
            "courier_id",
            "route_id",
            "event_type",
            "lat",
            "lon",
        ]
    ]

    client.insert_df("delivery_events", events)

    count = client.query("SELECT count() FROM delivery_events").result_rows[0][0]
    print(f"delivery_events: {count}")

    sample = client.query("""
        SELECT
            route_id,
            event_type,
            count() AS events_count
        FROM delivery_events
        GROUP BY route_id, event_type
        ORDER BY route_id, event_type
        LIMIT 20
    """)

    for row in sample.result_rows:
        print(row)

    print("ClickHouse loading finished successfully.")


if __name__ == "__main__":
    main()
