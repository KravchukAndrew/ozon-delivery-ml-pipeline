from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = Path(__file__).resolve().parents[1]


def make_bash_task(
    task_id: str,
    command: str,
    timeout_minutes: int = 30,
) -> BashOperator:
    """
    Каждая задача Airflow:
    1. заходит в корень проекта;
    2. активирует основное проектное окружение .venv;
    3. запускает нужную команду.

    Airflow стоит отдельно в .airflow-venv,
    но ML/ETL-код запускается из .venv.
    """
    bash_command = f"""
set -euo pipefail

cd "{PROJECT_DIR}"

echo "============================================================"
echo "TASK: {task_id}"
echo "============================================================"

echo "Current directory:"
pwd

echo "Activating project .venv..."
source .venv/bin/activate

echo "Python executable:"
which python

echo "Python version:"
python --version

echo "Running command:"
{command}
"""

    return BashOperator(
        task_id=task_id,
        bash_command=bash_command,
        execution_timeout=timedelta(minutes=timeout_minutes),
        append_env=True,
        env={
            "PYTHONUNBUFFERED": "1",
        },
    )


with DAG(
    dag_id="ozon_delivery_ml_pipeline",
    description="End-to-end local ML pipeline for courier route lateness prediction",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "bykas",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["ozon", "delivery", "ml", "spark", "grafana"],
) as dag:

    start_docker_services = make_bash_task(
        task_id="start_docker_services",
        command="""
docker compose up -d
docker compose ps
""",
        timeout_minutes=10,
    )

    generate_data = make_bash_task(
        task_id="generate_synthetic_delivery_data",
        command="""
python src/data_generation/generate_delivery_data.py
""",
        timeout_minutes=10,
    )

    load_postgres = make_bash_task(
        task_id="load_postgres_tables",
        command="""
python scripts/load_postgres.py
""",
        timeout_minutes=10,
    )

    load_clickhouse = make_bash_task(
        task_id="load_clickhouse_events",
        command="""
python scripts/load_clickhouse.py
""",
        timeout_minutes=10,
    )

    ensure_kafka_topic = make_bash_task(
        task_id="ensure_kafka_topic",
        command="""
docker exec ozon-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create \
  --if-not-exists \
  --topic delivery_events \
  --partitions 3 \
  --replication-factor 1

docker exec ozon-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --list
""",
        timeout_minutes=10,
    )

    send_events_to_kafka = make_bash_task(
        task_id="send_events_to_kafka",
        command="""
python scripts/send_delivery_events_to_kafka.py --limit 300 --sleep-ms 0
""",
        timeout_minutes=10,
    )

    update_redis_from_kafka = make_bash_task(
        task_id="update_redis_from_kafka",
        command="""
python scripts/update_redis_from_kafka.py \
  --max-messages 300 \
  --group-id "redis-updater-airflow-{{ ts_nodash }}"
""",
        timeout_minutes=15,
    )

    build_route_features = make_bash_task(
        task_id="build_route_features_with_pyspark",
        command="""
python src/features/build_route_features_spark.py
""",
        timeout_minutes=20,
    )

    train_lateness_model = make_bash_task(
        task_id="train_lateness_model",
        command="""
python src/models/train_lateness_model.py
""",
        timeout_minutes=20,
    )

    publish_route_predictions = make_bash_task(
        task_id="publish_route_predictions",
        command="""
python scripts/publish_route_predictions.py
""",
        timeout_minutes=10,
    )

    build_dashboard_tables = make_bash_task(
        task_id="build_dashboard_tables",
        command="""
python scripts/build_dashboard_tables.py
""",
        timeout_minutes=10,
    )

    regenerate_grafana_dashboard = make_bash_task(
        task_id="regenerate_grafana_dashboard",
        command="""
python scripts/create_grafana_dashboard.py
""",
        timeout_minutes=10,
    )

    restart_grafana = make_bash_task(
        task_id="restart_grafana",
        command="""
docker compose restart grafana
""",
        timeout_minutes=10,
    )

    start_docker_services >> generate_data

    generate_data >> load_postgres
    generate_data >> load_clickhouse
    generate_data >> ensure_kafka_topic
    generate_data >> build_route_features

    ensure_kafka_topic >> send_events_to_kafka >> update_redis_from_kafka

    build_route_features >> train_lateness_model >> publish_route_predictions

    [
        load_postgres,
        load_clickhouse,
        update_redis_from_kafka,
        publish_route_predictions,
    ] >> build_dashboard_tables

    build_dashboard_tables >> regenerate_grafana_dashboard >> restart_grafana
