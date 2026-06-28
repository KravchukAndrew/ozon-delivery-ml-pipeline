from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")

ROUTE_FEATURES_DIR = PROCESSED_DIR / "route_features"
ROUTE_FEATURES_SAMPLE_PATH = PROCESSED_DIR / "route_features_sample.csv"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ozon-route-features")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("Reading raw parquet files...")

    orders = spark.read.parquet(str(RAW_DIR / "orders.parquet"))
    routes = spark.read.parquet(str(RAW_DIR / "routes.parquet"))
    couriers = spark.read.parquet(str(RAW_DIR / "couriers.parquet"))
    events = spark.read.parquet(str(RAW_DIR / "delivery_events.parquet"))

    print("Input sizes:")
    print("orders:", orders.count())
    print("routes:", routes.count())
    print("couriers:", couriers.count())
    print("events:", events.count())

    # Агрегаты по заказам на уровне маршрута.
    # Это признаки и target-статистика, посчитанные из orders.
    orders_agg = (
        orders
        .groupBy("route_id")
        .agg(
            F.count("*").alias("orders_count_from_orders"),
            F.sum("item_count").alias("total_item_count"),
            F.avg("item_count").alias("avg_item_count"),
            F.sum("weight_kg").alias("total_weight_kg"),
            F.avg("weight_kg").alias("avg_weight_kg"),
            F.sum("volume_l").alias("total_volume_l"),
            F.avg("volume_l").alias("avg_volume_l"),
            F.avg("distance_to_prev_km").alias("avg_distance_to_prev_km"),
            F.max("distance_to_prev_km").alias("max_distance_to_prev_km"),
            F.sum("distance_to_prev_km").alias("sum_distance_to_prev_km"),
            F.avg("delay_minutes").alias("target_avg_delay_minutes_from_orders"),
            F.sum("is_late").alias("target_late_orders_from_orders"),
            F.avg("is_late").alias("target_lateness_rate_from_orders"),
        )
    )

    # Агрегаты по событиям.
    # Это скорее monitoring/event признаки, а не обязательно признаки для модели до старта маршрута.
    events_agg = (
        events
        .groupBy("route_id")
        .agg(
            F.count("*").alias("events_count"),
            F.countDistinct("event_type").alias("event_types_count"),
            F.min("event_time").alias("first_event_time"),
            F.max("event_time").alias("last_event_time"),
        )
        .withColumn(
            "event_stream_span_minutes",
            (
                F.unix_timestamp("last_event_time")
                - F.unix_timestamp("first_event_time")
            ) / 60.0,
        )
    )

    routes_selected = (
        routes
        .select(
            "route_id",
            "store_id",
            "courier_id",
            "zone_id",
            "route_date",
            "planned_start_at",
            "day_of_week",
            "start_hour",
            "weather",
            "is_rush_hour",
            "num_orders",
            "total_distance_km",
            "planned_duration_min",
            F.col("actual_duration_min").alias("target_actual_duration_min"),
            F.col("late_orders").alias("target_late_orders"),
            F.col("lateness_rate").alias("target_lateness_rate"),
            F.col("total_delay_minutes").alias("target_total_delay_minutes"),
        )
    )

    couriers_selected = (
        couriers
        .select(
            "courier_id",
            "vehicle_type",
            "capacity_orders",
            "base_speed_kmh",
            "experience_days",
            "historical_lateness_rate",
        )
    )

    features = (
        routes_selected
        .join(couriers_selected, on="courier_id", how="left")
        .join(orders_agg, on="route_id", how="left")
        .join(events_agg, on="route_id", how="left")
    )

    # Производные признаки.
    features = (
        features
        .withColumn(
            "capacity_utilization",
            F.round(F.col("num_orders") / F.col("capacity_orders"), 4),
        )
        .withColumn(
            "planned_minutes_per_order",
            F.round(F.col("planned_duration_min") / F.col("num_orders"), 4),
        )
        .withColumn(
            "planned_km_per_order",
            F.round(F.col("total_distance_km") / F.col("num_orders"), 4),
        )
        .withColumn(
            "weight_per_order",
            F.round(F.col("total_weight_kg") / F.col("num_orders"), 4),
        )
        .withColumn(
            "volume_per_order",
            F.round(F.col("total_volume_l") / F.col("num_orders"), 4),
        )
        .withColumn(
            "target_has_late_orders",
            (F.col("target_late_orders") > 0).cast("int"),
        )
        .withColumn(
            "target_bad_route",
            (F.col("target_lateness_rate") >= 0.3).cast("int"),
        )
    )

    final_columns = [
        "route_id",
        "store_id",
        "courier_id",
        "zone_id",
        "route_date",
        "planned_start_at",
        "day_of_week",
        "start_hour",
        "weather",
        "is_rush_hour",
        "vehicle_type",
        "capacity_orders",
        "base_speed_kmh",
        "experience_days",
        "historical_lateness_rate",
        "num_orders",
        "orders_count_from_orders",
        "total_distance_km",
        "sum_distance_to_prev_km",
        "avg_distance_to_prev_km",
        "max_distance_to_prev_km",
        "planned_duration_min",
        "planned_minutes_per_order",
        "planned_km_per_order",
        "capacity_utilization",
        "total_item_count",
        "avg_item_count",
        "total_weight_kg",
        "avg_weight_kg",
        "weight_per_order",
        "total_volume_l",
        "avg_volume_l",
        "volume_per_order",
        "events_count",
        "event_types_count",
        "event_stream_span_minutes",
        "target_actual_duration_min",
        "target_total_delay_minutes",
        "target_avg_delay_minutes_from_orders",
        "target_late_orders",
        "target_late_orders_from_orders",
        "target_lateness_rate",
        "target_lateness_rate_from_orders",
        "target_has_late_orders",
        "target_bad_route",
    ]

    features = features.select(*final_columns)

    print("Final feature table schema:")
    features.printSchema()

    print("Sample feature rows:")
    features.orderBy("route_id").show(10, truncate=False)

    print("Target distribution:")
    features.groupBy("target_has_late_orders").count().show()

    features_count = features.count()
    print(f"Final features count: {features_count}")

    # Spark пишет parquet как папку с part-файлами.
    if ROUTE_FEATURES_DIR.exists():
        shutil.rmtree(ROUTE_FEATURES_DIR)

    features.write.mode("overwrite").parquet(str(ROUTE_FEATURES_DIR))

    # Дополнительно сохраняем маленький CSV-сэмпл, чтобы удобно открыть глазами.
    sample_pdf = features.orderBy("route_id").limit(50).toPandas()
    sample_pdf.to_csv(ROUTE_FEATURES_SAMPLE_PATH, index=False)

    print()
    print("Route features saved successfully.")
    print(f"Parquet directory: {ROUTE_FEATURES_DIR.resolve()}")
    print(f"CSV sample:         {ROUTE_FEATURES_SAMPLE_PATH.resolve()}")

    spark.stop()


if __name__ == "__main__":
    main()
