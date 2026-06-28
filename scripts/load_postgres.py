from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


RAW_DIR = Path("data/raw")

POSTGRES_URL = "postgresql+psycopg2://ozon:ozon@127.0.0.1:5432/ozon"


def read_csv(name: str, parse_dates: list[str] | None = None) -> pd.DataFrame:
    path = RAW_DIR / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    return pd.read_csv(path, parse_dates=parse_dates)


def main() -> None:
    engine = create_engine(POSTGRES_URL)

    stores = read_csv("stores")
    couriers = read_csv("couriers")
    routes = read_csv(
        "routes",
        parse_dates=["route_date", "planned_start_at"],
    )
    orders = read_csv(
        "orders",
        parse_dates=[
            "order_created_at",
            "planned_start_at",
            "planned_arrival_at",
            "promised_delivery_at",
            "actual_delivery_at",
        ],
    )

    with engine.begin() as conn:
        # Удаляем старые учебные таблицы, если они были созданы ранее.
        conn.execute(text("DROP TABLE IF EXISTS orders"))
        conn.execute(text("DROP TABLE IF EXISTS routes"))
        conn.execute(text("DROP TABLE IF EXISTS couriers"))
        conn.execute(text("DROP TABLE IF EXISTS stores"))

    stores.to_sql("stores", engine, if_exists="replace", index=False)
    couriers.to_sql("couriers", engine, if_exists="replace", index=False)
    routes.to_sql("routes", engine, if_exists="replace", index=False)
    orders.to_sql("orders", engine, if_exists="replace", index=False)

    with engine.connect() as conn:
        for table in ["stores", "couriers", "routes", "orders"]:
            count = conn.execute(text(f"SELECT count(*) FROM {table}")).scalar()
            print(f"{table}: {count}")

    print("PostgreSQL loading finished successfully.")


if __name__ == "__main__":
    main()
