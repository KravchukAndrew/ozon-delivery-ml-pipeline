from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DASHBOARD_PATH = Path("grafana/dashboards/ozon_delivery_ml_monitoring.json")

DATASOURCE = {
    "type": "postgres",
    "uid": "ozon_postgres",
}


def make_target(raw_sql: str, ref_id: str = "A", fmt: str = "time_series") -> dict[str, Any]:
    return {
        "datasource": DATASOURCE,
        "editorMode": "code",
        "format": fmt,
        "rawQuery": True,
        "rawSql": raw_sql.strip(),
        "refId": ref_id,
    }


def make_stat_panel(
    panel_id: int,
    title: str,
    sql: str,
    x: int,
    y: int,
    w: int,
    h: int,
    unit: str = "short",
) -> dict[str, Any]:
    return {
        "id": panel_id,
        "type": "stat",
        "title": title,
        "datasource": DATASOURCE,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [make_target(sql, fmt="table")],
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "mappings": [],
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"color": "green", "value": None},
                        {"color": "yellow", "value": 0.5},
                        {"color": "red", "value": 0.8},
                    ],
                },
            },
            "overrides": [],
        },
        "options": {
            "reduceOptions": {
                "values": False,
                "calcs": ["lastNotNull"],
                "fields": "",
            },
            "orientation": "auto",
            "textMode": "auto",
            "wideLayout": True,
            "colorMode": "value",
            "graphMode": "area",
            "justifyMode": "auto",
            "showPercentChange": False,
        },
    }


def make_timeseries_panel(
    panel_id: int,
    title: str,
    sql: str,
    x: int,
    y: int,
    w: int,
    h: int,
    unit: str = "short",
) -> dict[str, Any]:
    return {
        "id": panel_id,
        "type": "timeseries",
        "title": title,
        "datasource": DATASOURCE,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [make_target(sql, fmt="time_series")],
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "custom": {
                    "drawStyle": "line",
                    "lineInterpolation": "linear",
                    "lineWidth": 2,
                    "fillOpacity": 10,
                    "showPoints": "auto",
                    "spanNulls": False,
                },
            },
            "overrides": [],
        },
        "options": {
            "legend": {
                "displayMode": "list",
                "placement": "bottom",
                "showLegend": True,
            },
            "tooltip": {
                "mode": "single",
                "sort": "none",
            },
        },
    }


def make_table_panel(
    panel_id: int,
    title: str,
    sql: str,
    x: int,
    y: int,
    w: int,
    h: int,
) -> dict[str, Any]:
    return {
        "id": panel_id,
        "type": "table",
        "title": title,
        "datasource": DATASOURCE,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [make_target(sql, fmt="table")],
        "fieldConfig": {
            "defaults": {
                "custom": {
                    "align": "auto",
                    "cellOptions": {
                        "type": "auto",
                    },
                },
                "mappings": [],
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"color": "green", "value": None},
                    ],
                },
            },
            "overrides": [],
        },
        "options": {
            "showHeader": True,
            "cellHeight": "sm",
            "footer": {
                "show": False,
                "reducer": ["sum"],
                "countRows": False,
            },
        },
    }


def make_bar_panel(
    panel_id: int,
    title: str,
    sql: str,
    x: int,
    y: int,
    w: int,
    h: int,
    unit: str = "short",
) -> dict[str, Any]:
    return {
        "id": panel_id,
        "type": "barchart",
        "title": title,
        "datasource": DATASOURCE,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [make_target(sql, fmt="table")],
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "mappings": [],
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"color": "green", "value": None},
                    ],
                },
            },
            "overrides": [],
        },
        "options": {
            "legend": {
                "displayMode": "list",
                "placement": "bottom",
                "showLegend": False,
            },
            "tooltip": {
                "mode": "single",
                "sort": "none",
            },
        },
    }


def build_dashboard() -> dict[str, Any]:
    panels = [
        make_stat_panel(
            panel_id=1,
            title="Total orders",
            sql="""
                SELECT
                    COALESCE(SUM(orders_count), 0) AS total_orders
                FROM daily_delivery_metrics;
            """,
            x=0,
            y=0,
            w=6,
            h=4,
        ),
        make_stat_panel(
            panel_id=2,
            title="Average lateness rate",
            sql="""
                SELECT
                    COALESCE(AVG(lateness_rate), 0) AS average_lateness_rate
                FROM daily_delivery_metrics;
            """,
            x=6,
            y=0,
            w=6,
            h=4,
            unit="percentunit",
        ),
        make_stat_panel(
            panel_id=3,
            title="Average P90 delay minutes",
            sql="""
                SELECT
                    COALESCE(AVG(p90_delay_minutes), 0) AS average_p90_delay_minutes
                FROM daily_delivery_metrics;
            """,
            x=12,
            y=0,
            w=6,
            h=4,
        ),
        make_stat_panel(
            panel_id=4,
            title="High-risk routes",
            sql="""
                SELECT
                    COUNT(*) AS high_risk_routes
                FROM route_risk_summary
                WHERE risk_bucket = 'high';
            """,
            x=18,
            y=0,
            w=6,
            h=4,
        ),
        make_timeseries_panel(
            panel_id=5,
            title="Orders count by day",
            sql="""
                SELECT
                    delivery_date::timestamp AS "time",
                    orders_count AS orders_count
                FROM daily_delivery_metrics
                ORDER BY delivery_date;
            """,
            x=0,
            y=4,
            w=12,
            h=8,
        ),
        make_timeseries_panel(
            panel_id=6,
            title="Lateness rate by day",
            sql="""
                SELECT
                    delivery_date::timestamp AS "time",
                    lateness_rate AS lateness_rate
                FROM daily_delivery_metrics
                ORDER BY delivery_date;
            """,
            x=12,
            y=4,
            w=12,
            h=8,
            unit="percentunit",
        ),
        make_timeseries_panel(
            panel_id=7,
            title="P90 delay minutes by day",
            sql="""
                SELECT
                    delivery_date::timestamp AS "time",
                    p90_delay_minutes AS p90_delay_minutes
                FROM daily_delivery_metrics
                ORDER BY delivery_date;
            """,
            x=0,
            y=12,
            w=12,
            h=8,
        ),
        make_timeseries_panel(
            panel_id=8,
            title="Precision and recall by day",
            sql="""
                SELECT
                    route_date::timestamp AS "time",
                    COALESCE(precision_at_05, 0) AS precision_at_05,
                    COALESCE(recall_at_05, 0) AS recall_at_05
                FROM model_quality_by_day
                ORDER BY route_date;
            """,
            x=12,
            y=12,
            w=12,
            h=8,
            unit="percentunit",
        ),
        make_bar_panel(
            panel_id=9,
            title="Routes by risk bucket",
            sql="""
                SELECT
                    risk_bucket,
                    COUNT(*) AS routes_count
                FROM route_risk_summary
                GROUP BY risk_bucket
                ORDER BY
                    CASE risk_bucket
                        WHEN 'high' THEN 1
                        WHEN 'medium' THEN 2
                        WHEN 'low' THEN 3
                        ELSE 4
                    END;
            """,
            x=0,
            y=20,
            w=8,
            h=8,
        ),
        make_timeseries_panel(
            panel_id=10,
            title="Brier score by day",
            sql="""
                SELECT
                    route_date::timestamp AS "time",
                    brier_score AS brier_score
                FROM model_quality_by_day
                ORDER BY route_date;
            """,
            x=8,
            y=20,
            w=8,
            h=8,
        ),
        make_bar_panel(
            panel_id=11,
            title="Confusion matrix totals",
            sql="""
                SELECT 'true_positive' AS result, COALESCE(SUM(true_positive), 0) AS routes_count
                FROM model_quality_by_day

                UNION ALL

                SELECT 'false_positive' AS result, COALESCE(SUM(false_positive), 0) AS routes_count
                FROM model_quality_by_day

                UNION ALL

                SELECT 'false_negative' AS result, COALESCE(SUM(false_negative), 0) AS routes_count
                FROM model_quality_by_day

                UNION ALL

                SELECT 'true_negative' AS result, COALESCE(SUM(true_negative), 0) AS routes_count
                FROM model_quality_by_day;
            """,
            x=16,
            y=20,
            w=8,
            h=8,
        ),
        make_table_panel(
            panel_id=12,
            title="Top risky routes",
            sql="""
                SELECT
                    route_id,
                    route_date,
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
                LIMIT 20;
            """,
            x=0,
            y=28,
            w=24,
            h=10,
        ),
    ]

    return {
        "id": None,
        "uid": "ozon-delivery-ml",
        "title": "Ozon Delivery & ML Monitoring",
        "tags": ["ozon", "delivery", "ml", "monitoring"],
        "timezone": "browser",
        "schemaVersion": 39,
        "version": 1,
        "refresh": "30s",
        "editable": True,
        "graphTooltip": 0,
        "fiscalYearStartMonth": 0,
        "time": {
            "from": "now-180d",
            "to": "now+180d",
        },
        "timepicker": {},
        "templating": {
            "list": [],
        },
        "annotations": {
            "list": [
                {
                    "builtIn": 1,
                    "datasource": {
                        "type": "grafana",
                        "uid": "-- Grafana --",
                    },
                    "enable": True,
                    "hide": True,
                    "iconColor": "rgba(0, 211, 255, 1)",
                    "name": "Annotations & Alerts",
                    "type": "dashboard",
                }
            ]
        },
        "links": [],
        "panels": panels,
    }


def main() -> None:
    DASHBOARD_PATH.parent.mkdir(parents=True, exist_ok=True)

    dashboard = build_dashboard()

    with open(DASHBOARD_PATH, "w", encoding="utf-8") as f:
        json.dump(dashboard, f, indent=2, ensure_ascii=False)

    print(f"Grafana dashboard written to: {DASHBOARD_PATH}")


if __name__ == "__main__":
    main()
