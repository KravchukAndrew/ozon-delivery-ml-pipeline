from __future__ import annotations

import os
from typing import Any

import redis
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text


POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql+psycopg2://ozon:ozon@127.0.0.1:5432/ozon",
)

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


app = FastAPI(
    title="Ozon Delivery ML API",
    description="Local API for courier route lateness prediction",
    version="0.1.0",
)

engine = create_engine(
    POSTGRES_URL,
    pool_pre_ping=True,
)

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
)


def row_to_dict(row: Any) -> dict[str, Any]:
    return dict(row._mapping)


@app.get("/")
def root() -> dict[str, str]:
    return {
        "service": "Ozon Delivery ML API",
        "status": "running",
        "docs": "/docs",
    }


@app.get("/health")
def health() -> dict[str, Any]:
    checks: dict[str, Any] = {}

    try:
        with engine.begin() as conn:
            postgres_result = conn.execute(text("SELECT 1")).scalar()
        checks["postgres"] = postgres_result == 1
    except Exception as exc:
        checks["postgres"] = False
        checks["postgres_error"] = str(exc)

    try:
        redis_result = redis_client.ping()
        checks["redis"] = bool(redis_result)
    except Exception as exc:
        checks["redis"] = False
        checks["redis_error"] = str(exc)

    checks["status"] = "ok" if checks.get("postgres") and checks.get("redis") else "degraded"

    return checks


@app.get("/routes/{route_id}/prediction")
def get_route_prediction(route_id: int) -> dict[str, Any]:
    """
    Возвращает прогноз риска опоздания для конкретного маршрута.

    Сначала пробуем Redis, потому что это быстрый кэш.
    Если в Redis нет такого route_id, идём в PostgreSQL.
    """
    redis_key = f"route:{route_id}:prediction"
    redis_data = redis_client.hgetall(redis_key)

    if redis_data:
        return {
            "source": "redis",
            "route_id": route_id,
            "prediction": redis_data,
        }

    query = text(
        """
        SELECT
            route_id,
            route_date,
            courier_id,
            zone_id,
            weather,
            vehicle_type,
            num_orders,
            target_has_late_orders,
            target_lateness_rate,
            predicted_late_probability,
            predicted_has_late_orders,
            risk_bucket,
            prediction_result,
            model_version,
            prediction_created_at
        FROM route_risk_summary
        WHERE route_id = :route_id
        LIMIT 1
        """
    )

    with engine.begin() as conn:
        row = conn.execute(query, {"route_id": route_id}).fetchone()

    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Prediction for route_id={route_id} not found",
        )

    return {
        "source": "postgres",
        "route_id": route_id,
        "prediction": row_to_dict(row),
    }


@app.get("/routes/top-risky")
def get_top_risky_routes(
    limit: int = Query(default=10, ge=1, le=100),
) -> dict[str, Any]:
    query = text(
        """
        SELECT
            route_id,
            route_date,
            courier_id,
            zone_id,
            weather,
            vehicle_type,
            num_orders,
            ROUND(predicted_late_probability::numeric, 4) AS predicted_late_probability,
            target_has_late_orders,
            ROUND(target_lateness_rate::numeric, 4) AS target_lateness_rate,
            risk_bucket,
            prediction_result
        FROM route_risk_summary
        ORDER BY predicted_late_probability DESC
        LIMIT :limit
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(query, {"limit": limit}).fetchall()

    return {
        "limit": limit,
        "routes": [row_to_dict(row) for row in rows],
    }


@app.get("/metrics/summary")
def get_metrics_summary() -> dict[str, Any]:
    delivery_query = text(
        """
        SELECT
            COALESCE(SUM(orders_count), 0) AS total_orders,
            COALESCE(SUM(routes_count), 0) AS total_routes,
            COALESCE(AVG(lateness_rate), 0) AS avg_lateness_rate,
            COALESCE(AVG(p90_delay_minutes), 0) AS avg_p90_delay_minutes
        FROM daily_delivery_metrics
        """
    )

    risk_query = text(
        """
        SELECT
            risk_bucket,
            COUNT(*) AS routes_count
        FROM route_risk_summary
        GROUP BY risk_bucket
        ORDER BY routes_count DESC
        """
    )

    quality_query = text(
        """
        SELECT
            COALESCE(AVG(precision_at_05), 0) AS avg_precision_at_05,
            COALESCE(AVG(recall_at_05), 0) AS avg_recall_at_05,
            COALESCE(AVG(brier_score), 0) AS avg_brier_score
        FROM model_quality_by_day
        """
    )

    with engine.begin() as conn:
        delivery = row_to_dict(conn.execute(delivery_query).fetchone())
        risk_buckets = [row_to_dict(row) for row in conn.execute(risk_query).fetchall()]
        model_quality = row_to_dict(conn.execute(quality_query).fetchone())

    return {
        "delivery": delivery,
        "risk_buckets": risk_buckets,
        "model_quality": model_quality,
    }
