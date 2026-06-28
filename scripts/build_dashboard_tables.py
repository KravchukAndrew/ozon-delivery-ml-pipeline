from __future__ import annotations

from sqlalchemy import create_engine, text


POSTGRES_URL = "postgresql+psycopg2://ozon:ozon@127.0.0.1:5432/ozon"


def main() -> None:
    engine = create_engine(POSTGRES_URL)

    with engine.begin() as conn:
        print("Dropping old dashboard tables...")

        conn.execute(text("DROP TABLE IF EXISTS daily_delivery_metrics"))
        conn.execute(text("DROP TABLE IF EXISTS route_risk_summary"))
        conn.execute(text("DROP TABLE IF EXISTS model_quality_by_day"))

        print("Creating daily_delivery_metrics...")

        conn.execute(
            text(
                """
                CREATE TABLE daily_delivery_metrics AS
                SELECT
                    DATE(o.planned_start_at) AS delivery_date,

                    COUNT(*) AS orders_count,
                    COUNT(DISTINCT o.route_id) AS routes_count,
                    COUNT(DISTINCT o.courier_id) AS couriers_count,

                    AVG(o.delay_minutes) AS avg_delay_minutes,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY o.delay_minutes) AS p50_delay_minutes,
                    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY o.delay_minutes) AS p90_delay_minutes,

                    SUM(o.is_late) AS late_orders_count,
                    AVG(o.is_late::float) AS lateness_rate,

                    AVG(o.weight_kg) AS avg_weight_kg,
                    SUM(o.weight_kg) AS total_weight_kg,
                    AVG(o.volume_l) AS avg_volume_l,
                    SUM(o.volume_l) AS total_volume_l

                FROM orders o
                GROUP BY DATE(o.planned_start_at)
                ORDER BY delivery_date
                """
            )
        )

        print("Creating route_risk_summary...")

        conn.execute(
            text(
                """
                CREATE TABLE route_risk_summary AS
                SELECT
                    rp.route_id,
                    rp.route_date,
                    rp.courier_id,
                    rp.zone_id,
                    rp.weather,
                    rp.vehicle_type,
                    rp.num_orders,

                    rp.target_has_late_orders,
                    rp.target_lateness_rate,
                    rp.predicted_late_probability,
                    rp.predicted_has_late_orders,
                    rp.model_version,
                    rp.prediction_created_at,

                    CASE
                        WHEN rp.predicted_late_probability >= 0.8 THEN 'high'
                        WHEN rp.predicted_late_probability >= 0.5 THEN 'medium'
                        ELSE 'low'
                    END AS risk_bucket,

                    CASE
                        WHEN rp.predicted_has_late_orders = 1 AND rp.target_has_late_orders = 1 THEN 'true_positive'
                        WHEN rp.predicted_has_late_orders = 1 AND rp.target_has_late_orders = 0 THEN 'false_positive'
                        WHEN rp.predicted_has_late_orders = 0 AND rp.target_has_late_orders = 1 THEN 'false_negative'
                        WHEN rp.predicted_has_late_orders = 0 AND rp.target_has_late_orders = 0 THEN 'true_negative'
                        ELSE 'unknown'
                    END AS prediction_result

                FROM route_predictions rp
                ORDER BY rp.predicted_late_probability DESC
                """
            )
        )

        print("Creating model_quality_by_day...")

        conn.execute(
            text(
                """
                CREATE TABLE model_quality_by_day AS
                SELECT
                    route_date,

                    COUNT(*) AS routes_count,

                    AVG(target_has_late_orders::float) AS actual_bad_route_rate,
                    AVG(predicted_late_probability) AS avg_predicted_late_probability,

                    AVG(
                        POWER(predicted_late_probability - target_has_late_orders::float, 2)
                    ) AS brier_score,

                    SUM(
                        CASE
                            WHEN predicted_has_late_orders = 1
                             AND target_has_late_orders = 1
                            THEN 1 ELSE 0
                        END
                    ) AS true_positive,

                    SUM(
                        CASE
                            WHEN predicted_has_late_orders = 1
                             AND target_has_late_orders = 0
                            THEN 1 ELSE 0
                        END
                    ) AS false_positive,

                    SUM(
                        CASE
                            WHEN predicted_has_late_orders = 0
                             AND target_has_late_orders = 1
                            THEN 1 ELSE 0
                        END
                    ) AS false_negative,

                    SUM(
                        CASE
                            WHEN predicted_has_late_orders = 0
                             AND target_has_late_orders = 0
                            THEN 1 ELSE 0
                        END
                    ) AS true_negative,

                    SUM(
                        CASE
                            WHEN predicted_has_late_orders = 1
                             AND target_has_late_orders = 1
                            THEN 1 ELSE 0
                        END
                    )::float
                    /
                    NULLIF(
                        SUM(
                            CASE
                                WHEN predicted_has_late_orders = 1
                                THEN 1 ELSE 0
                            END
                        ),
                        0
                    ) AS precision_at_05,

                    SUM(
                        CASE
                            WHEN predicted_has_late_orders = 1
                             AND target_has_late_orders = 1
                            THEN 1 ELSE 0
                        END
                    )::float
                    /
                    NULLIF(
                        SUM(
                            CASE
                                WHEN target_has_late_orders = 1
                                THEN 1 ELSE 0
                            END
                        ),
                        0
                    ) AS recall_at_05

                FROM route_predictions
                GROUP BY route_date
                ORDER BY route_date
                """
            )
        )

        print("Creating indexes...")

        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_daily_delivery_metrics_date
                ON daily_delivery_metrics(delivery_date)
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_route_risk_summary_probability
                ON route_risk_summary(predicted_late_probability)
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_route_risk_summary_bucket
                ON route_risk_summary(risk_bucket)
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_model_quality_by_day_date
                ON model_quality_by_day(route_date)
                """
            )
        )

        print()
        print("Dashboard tables created.")

        for table in [
            "daily_delivery_metrics",
            "route_risk_summary",
            "model_quality_by_day",
        ]:
            count = conn.execute(text(f"SELECT count(*) FROM {table}")).scalar()
            print(f"{table}: {count} rows")


if __name__ == "__main__":
    main()
