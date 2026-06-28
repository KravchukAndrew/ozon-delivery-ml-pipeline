from __future__ import annotations

from pathlib import Path

import pandas as pd
import redis
import clickhouse_connect
from sqlalchemy import create_engine, text


PREDICTIONS_PATH = Path("artifacts/predictions/route_predictions.csv")

POSTGRES_URL = "postgresql+psycopg2://ozon:ozon@127.0.0.1:5432/ozon"

CLICKHOUSE_HOST = "127.0.0.1"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "ozon"
CLICKHOUSE_DB = "ozon"

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379

MODEL_VERSION = "baseline_lateness_model_v1"


def load_predictions() -> pd.DataFrame:
    if not PREDICTIONS_PATH.exists():
        raise FileNotFoundError(
            f"File not found: {PREDICTIONS_PATH}. "
            "Сначала запусти обучение модели."
        )

    df = pd.read_csv(PREDICTIONS_PATH)

    required_columns = [
        "route_id",
        "route_date",
        "courier_id",
        "zone_id",
        "weather",
        "vehicle_type",
        "num_orders",
        "target_has_late_orders",
        "target_lateness_rate",
        "predicted_late_probability",
        "predicted_has_late_orders",
    ]

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in predictions: {missing}")

    df["model_version"] = MODEL_VERSION
    df["prediction_created_at"] = pd.Timestamp.now().floor("s")

    return df


def write_to_postgres(df: pd.DataFrame) -> None:
    """
    PostgreSQL — основная таблица с последними предсказаниями модели.
    Её удобно использовать для API, аналитики, DBeaver и будущего дашборда.
    """
    engine = create_engine(POSTGRES_URL)

    postgres_df = df.copy()

    postgres_df.to_sql(
        "route_predictions",
        engine,
        if_exists="replace",
        index=False,
    )

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_route_predictions_route_id
                ON route_predictions(route_id)
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_route_predictions_probability
                ON route_predictions(predicted_late_probability)
                """
            )
        )

        count = conn.execute(
            text("SELECT count(*) FROM route_predictions")
        ).scalar()

    print(f"PostgreSQL route_predictions rows: {count}")


def write_to_redis(df: pd.DataFrame) -> None:
    """
    Redis — быстрый кэш скорингов.

    Пример ключей:
    route:123:late_probability
    route:123:prediction
    """
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )

    pipe = r.pipeline()

    for _, row in df.iterrows():
        route_id = int(row["route_id"])

        probability_key = f"route:{route_id}:late_probability"
        prediction_hash_key = f"route:{route_id}:prediction"

        probability = float(row["predicted_late_probability"])

        pipe.set(probability_key, probability)

        pipe.hset(
            prediction_hash_key,
            mapping={
                "route_id": str(route_id),
                "route_date": str(row["route_date"]),
                "courier_id": str(row["courier_id"]),
                "zone_id": str(row["zone_id"]),
                "weather": str(row["weather"]),
                "vehicle_type": str(row["vehicle_type"]),
                "num_orders": str(row["num_orders"]),
                "target_has_late_orders": str(row["target_has_late_orders"]),
                "target_lateness_rate": str(row["target_lateness_rate"]),
                "predicted_late_probability": str(probability),
                "predicted_has_late_orders": str(row["predicted_has_late_orders"]),
                "model_version": str(row["model_version"]),
                "prediction_created_at": str(row["prediction_created_at"]),
            },
        )

    pipe.execute()

    print(f"Redis predictions written for routes: {len(df)}")


def write_to_clickhouse(df: pd.DataFrame) -> None:
    """
    ClickHouse — событийная таблица с фактами скоринга.

    В реальном проекте туда удобно писать историю всех запусков модели:
    когда модель сделала прогноз, какой route_id, какая вероятность.
    """
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )

    client.command("DROP TABLE IF EXISTS route_prediction_events")

    client.command(
        """
        CREATE TABLE route_prediction_events (
            route_id UInt64,
            route_date String,
            courier_id UInt64,
            zone_id String,
            weather String,
            vehicle_type String,
            num_orders UInt64,
            target_has_late_orders UInt8,
            target_lateness_rate Float64,
            predicted_late_probability Float64,
            predicted_has_late_orders UInt8,
            model_version String,
            prediction_created_at DateTime
        )
        ENGINE = MergeTree
        ORDER BY (prediction_created_at, route_id)
        """
    )

    clickhouse_df = df[
        [
            "route_id",
            "route_date",
            "courier_id",
            "zone_id",
            "weather",
            "vehicle_type",
            "num_orders",
            "target_has_late_orders",
            "target_lateness_rate",
            "predicted_late_probability",
            "predicted_has_late_orders",
            "model_version",
            "prediction_created_at",
        ]
    ].copy()

    clickhouse_df["route_id"] = clickhouse_df["route_id"].astype("uint64")
    clickhouse_df["courier_id"] = clickhouse_df["courier_id"].astype("uint64")
    clickhouse_df["num_orders"] = clickhouse_df["num_orders"].astype("uint64")
    clickhouse_df["target_has_late_orders"] = clickhouse_df["target_has_late_orders"].astype("uint8")
    clickhouse_df["predicted_has_late_orders"] = clickhouse_df["predicted_has_late_orders"].astype("uint8")
    clickhouse_df["target_lateness_rate"] = clickhouse_df["target_lateness_rate"].astype("float64")
    clickhouse_df["predicted_late_probability"] = clickhouse_df["predicted_late_probability"].astype("float64")
    clickhouse_df["prediction_created_at"] = pd.to_datetime(clickhouse_df["prediction_created_at"])

    client.insert_df("route_prediction_events", clickhouse_df)

    count = client.query("SELECT count() FROM route_prediction_events").result_rows[0][0]
    print(f"ClickHouse route_prediction_events rows: {count}")


def main() -> None:
    df = load_predictions()

    print("Loaded predictions:")
    print(df.shape)
    print(df.head(5).to_string(index=False))
    print()

    write_to_postgres(df)
    write_to_redis(df)
    write_to_clickhouse(df)

    print()
    print("Route predictions published successfully.")


if __name__ == "__main__":
    main()
