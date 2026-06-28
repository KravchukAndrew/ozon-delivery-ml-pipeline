# Ozon Delivery ML Pipeline

Production-like локальный проект для задачи прогнозирования риска опозданий курьерских маршрутов в e-commerce / fresh-доставке.

Проект имитирует полный путь данных:

```text
synthetic raw data
→ PostgreSQL / ClickHouse / Kafka / Redis
→ PySpark feature engineering
→ ML model training
→ prediction publishing
→ FastAPI serving
→ Grafana monitoring
→ Airflow orchestration
```

Цель проекта — показать end-to-end ML pipeline, близкий к реальному промышленному контуру: от сырых данных и потоковых событий до модели, API, мониторинга и оркестрации.

---

## Содержание

1. [Бизнес-задача](#1-бизнес-задача)
2. [Архитектура проекта](#2-архитектура-проекта)
3. [Стек технологий](#3-стек-технологий)
4. [Структура проекта](#4-структура-проекта)
5. [Основные сервисы](#5-основные-сервисы)
6. [Быстрый старт](#6-быстрый-старт)
7. [Airflow DAG](#7-airflow-dag)
8. [Данные](#8-данные)
9. [PostgreSQL tables](#9-postgresql-tables)
10. [ClickHouse tables](#10-clickhouse-tables)
11. [Kafka](#11-kafka)
12. [Redis](#12-redis)
13. [PySpark feature pipeline](#13-pyspark-feature-pipeline)
14. [ML model](#14-ml-model)
15. [Prediction publishing](#15-prediction-publishing)
16. [Dashboard tables](#16-dashboard-tables)
17. [Grafana](#17-grafana)
18. [FastAPI](#18-fastapi)
19. [Как остановить проект](#19-как-остановить-проект)
20. [Как продолжить работу после перезагрузки](#20-как-продолжить-работу-после-перезагрузки)
21. [Резюме для собеседования](#21-резюме-для-собеседования)
22. [Возможные улучшения](#22-возможные-улучшения)
23. [Git](#23-git)

---

## 1. Бизнес-задача

В логистике курьерской доставки важно заранее понимать, какие маршруты с высокой вероятностью будут проблемными:

- курьер может опоздать к клиенту;
- маршрут может быть перегружен;
- погодные условия могут ухудшить SLA;
- в отдельных зонах может систематически расти задержка;
- операторам нужно заранее видеть топ рискованных маршрутов.

В этом проекте решается задача:

```text
по признакам маршрута предсказать вероятность того,
что в маршруте будет хотя бы один опоздавший заказ
```

Target:

```text
target_has_late_orders
```

То есть:

```text
1 — в маршруте был хотя бы один опоздавший заказ
0 — маршрут прошёл без опозданий
```

---

## 2. Архитектура проекта

Общая схема:

```text
┌────────────────────────────┐
│ Synthetic Data Generator   │
│ stores, couriers, routes,  │
│ orders, delivery_events    │
└──────────────┬─────────────┘
               │
               ▼
┌────────────────────────────┐
│ data/raw                   │
│ CSV + Parquet              │
└───────┬─────────┬──────────┘
        │         │
        │         ▼
        │   ┌────────────────────┐
        │   │ ClickHouse          │
        │   │ delivery_events     │
        │   │ prediction_events   │
        │   └────────────────────┘
        │
        ▼
┌────────────────────────────┐
│ PostgreSQL                 │
│ stores, couriers, routes,  │
│ orders, predictions, marts │
└──────────────┬─────────────┘
               │
               ▼
┌────────────────────────────┐
│ PySpark Feature Pipeline   │
│ route-level feature table  │
└──────────────┬─────────────┘
               │
               ▼
┌────────────────────────────┐
│ ML Training                │
│ Logistic Regression        │
│ Random Forest              │
└──────────────┬─────────────┘
               │
               ▼
┌────────────────────────────┐
│ Prediction Publishing      │
│ PostgreSQL / Redis / CH    │
└───────┬───────────┬────────┘
        │           │
        ▼           ▼
┌──────────────┐ ┌────────────────┐
│ FastAPI      │ │ Grafana         │
│ prediction   │ │ monitoring      │
│ serving      │ │ dashboard       │
└──────────────┘ └────────────────┘
```

Дополнительно:

- `Kafka → Redis consumer` обновляет state маршрутов и курьеров.
- `Airflow` оркестрирует весь pipeline.

---

## 3. Стек технологий

### Data storage

- `PostgreSQL` — структурированные таблицы, predictions, dashboard marts.
- `ClickHouse` — событийное аналитическое хранилище.
- `Redis` — быстрый кэш состояния маршрутов и ML-предсказаний.
- `Parquet / CSV` — raw и processed data layer.

### Streaming

- `Kafka` — поток событий доставки.
- `Kafka UI` — интерфейс для просмотра topic-ов.

### Data processing

- `Python`
- `pandas`
- `PySpark`
- `SQLAlchemy`

### Machine Learning

- `scikit-learn`
- `Logistic Regression`
- `Random Forest`
- `time-based split`
- `ROC-AUC`
- `PR-AUC`
- `LogLoss`
- `Brier score`
- `Precision@K / Recall@K`

### Serving

- `FastAPI`
- `Uvicorn`
- `Docker Compose`

### Monitoring

- `Grafana`
- `PostgreSQL datasource`
- dashboard provisioning as code

### Orchestration

- `Apache Airflow`
- `Airflow DAG`
- `BashOperator`

---

## 4. Структура проекта

```text
ozon-local/
├── dags/
│   └── ozon_delivery_ml_pipeline_dag.py
│
├── grafana/
│   ├── dashboards/
│   │   └── ozon_delivery_ml_monitoring.json
│   └── provisioning/
│       ├── dashboards/
│       │   └── ozon-dashboard.yml
│       └── datasources/
│           └── postgres.yml
│
├── scripts/
│   ├── build_dashboard_tables.py
│   ├── create_grafana_dashboard.py
│   ├── consume_delivery_events_from_kafka.py
│   ├── load_clickhouse.py
│   ├── load_postgres.py
│   ├── publish_route_predictions.py
│   ├── send_delivery_events_to_kafka.py
│   ├── start_airflow.sh
│   └── update_redis_from_kafka.py
│
├── src/
│   ├── api/
│   │   └── main.py
│   ├── data_generation/
│   │   └── generate_delivery_data.py
│   ├── features/
│   │   └── build_route_features_spark.py
│   └── models/
│       └── train_lateness_model.py
│
├── data/
│   ├── raw/
│   └── processed/
│
├── artifacts/
│   ├── models/
│   ├── predictions/
│   └── reports/
│
├── docker-compose.yml
├── docker-compose.override.yml
├── Dockerfile.api
├── requirements-api.txt
├── .gitignore
└── README.md
```

---

## 5. Основные сервисы

После запуска:

```bash
docker compose up -d
```

доступны сервисы:

| Сервис       | URL / host                    | Назначение              |
| ------------ | ----------------------------- | ----------------------- |
| PostgreSQL   | `127.0.0.1:5432`              | Основная relational DB  |
| ClickHouse   | `127.0.0.1:8123`              | Событийная аналитика    |
| Kafka        | `127.0.0.1:9092`              | Поток delivery events   |
| Kafka UI     | `http://localhost:8082`       | Интерфейс Kafka         |
| Redis        | `127.0.0.1:6379`              | Кэш state и predictions |
| RedisInsight | `http://localhost:5540`       | Интерфейс Redis         |
| Grafana      | `http://localhost:3000`       | Monitoring dashboard    |
| FastAPI      | `http://localhost:8000`       | Prediction serving      |
| FastAPI docs | `http://localhost:8000/docs`  | Swagger UI              |
| Airflow      | `http://localhost:8080`       | Pipeline orchestration  |

---

## 6. Быстрый старт

### 6.1. Перейти в проект

```bash
cd ~/projects/ozon-local
```

### 6.2. Активировать основное окружение проекта

```bash
source .venv/bin/activate
```

### 6.3. Запустить Docker-сервисы

```bash
docker compose up -d
docker compose ps
```

### 6.4. Запустить Airflow

В отдельном терминале:

```bash
cd ~/projects/ozon-local
./scripts/start_airflow.sh
```

Airflow UI:

```text
http://localhost:8080
```

Логин:

```text
admin
```

Пароль:

```bash
cat airflow_home/standalone_admin_password.txt
```

---

## 7. Airflow DAG

Основной DAG:

```text
ozon_delivery_ml_pipeline
```

Файл:

```text
dags/ozon_delivery_ml_pipeline_dag.py
```

DAG запускает полный ML pipeline:

```text
start_docker_services
    ↓
generate_synthetic_delivery_data
    ↓
load_postgres_tables
load_clickhouse_events
ensure_kafka_topic
build_route_features_with_pyspark
    ↓
send_events_to_kafka
    ↓
update_redis_from_kafka
    ↓
train_lateness_model
    ↓
publish_route_predictions
    ↓
build_dashboard_tables
    ↓
regenerate_grafana_dashboard
    ↓
restart_grafana
restart_api
```

Запуск DAG:

1. Открыть Airflow UI.
2. Найти `ozon_delivery_ml_pipeline`.
3. Включить DAG, если он `paused`.
4. Нажать `Trigger DAG`.
5. Проверить, что все задачи стали зелёными.

---

## 8. Данные

Генератор создаёт синтетические данные:

```text
src/data_generation/generate_delivery_data.py
```

Сущности:

- `stores`
- `couriers`
- `routes`
- `orders`
- `delivery_events`

Файлы сохраняются в:

```text
data/raw/
```

Форматы:

- `CSV`
- `Parquet`

Parquet сохраняется в Spark-compatible формате с timestamp microseconds, чтобы PySpark корректно читал данные.

---

## 9. PostgreSQL tables

В PostgreSQL создаются таблицы.

### Raw / operational tables

- `stores`
- `couriers`
- `routes`
- `orders`

### ML prediction tables

- `route_predictions`

### Dashboard marts

- `daily_delivery_metrics`
- `route_risk_summary`
- `model_quality_by_day`

Проверка:

```bash
docker exec -it ozon-postgres psql -U ozon -d ozon
```

Примеры SQL:

```sql
SELECT count(*) FROM orders;
SELECT count(*) FROM route_predictions;
SELECT count(*) FROM daily_delivery_metrics;
SELECT count(*) FROM route_risk_summary;
SELECT count(*) FROM model_quality_by_day;
```

Топ рискованных маршрутов:

```sql
SELECT
    route_id,
    route_date,
    predicted_late_probability,
    target_has_late_orders,
    risk_bucket,
    prediction_result
FROM route_risk_summary
ORDER BY predicted_late_probability DESC
LIMIT 10;
```

---

## 10. ClickHouse tables

В ClickHouse создаются:

- `delivery_events`
- `route_prediction_events`

Проверка:

```bash
docker exec -it ozon-clickhouse clickhouse-client --user default --password ozon --database ozon
```

Пример:

```sql
SELECT count() FROM delivery_events;

SELECT
    event_type,
    count() AS events_count
FROM delivery_events
GROUP BY event_type
ORDER BY events_count DESC;
```

---

## 11. Kafka

Kafka topic:

```text
delivery_events
```

Проверить topic-и:

```bash
docker exec -it ozon-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --list
```

Сервисный topic:

```text
__consumer_offsets
```

Это нормально. Kafka использует его для хранения offset-ов consumer groups.

---

## 12. Redis

Redis хранит state и predictions.

Примеры ключей:

```text
route:{route_id}:state
courier:{courier_id}:state
route:{route_id}:events_count
route:{route_id}:last_event_json
route:{route_id}:late_probability
route:{route_id}:prediction
```

Проверка:

```bash
docker exec -it ozon-redis redis-cli
```

Внутри Redis:

```redis
SCAN 0 MATCH route:*:late_probability COUNT 20
GET route:245:late_probability
HGETALL route:245:prediction
```

---

## 13. PySpark feature pipeline

Файл:

```text
src/features/build_route_features_spark.py
```

Вход:

```text
data/raw/orders.parquet
data/raw/routes.parquet
data/raw/couriers.parquet
data/raw/delivery_events.parquet
```

Выход:

```text
data/processed/route_features/
data/processed/route_features_sample.csv
```

Одна строка в feature table соответствует одному маршруту:

```text
one row = one route_id
```

Примеры признаков:

- `zone_id`
- `day_of_week`
- `start_hour`
- `weather`
- `vehicle_type`
- `capacity_orders`
- `base_speed_kmh`
- `experience_days`
- `historical_lateness_rate`
- `num_orders`
- `total_distance_km`
- `planned_duration_min`
- `capacity_utilization`
- `total_weight_kg`
- `total_volume_l`

Target-колонки:

- `target_has_late_orders`
- `target_lateness_rate`
- `target_bad_route`
- `target_total_delay_minutes`

Важно: target-колонки не используются как признаки модели, чтобы не было data leakage.

---

## 14. ML model

Файл:

```text
src/models/train_lateness_model.py
```

Модель решает задачу бинарной классификации:

```text
предсказать, будет ли в маршруте хотя бы один опоздавший заказ
```

Target:

```text
target_has_late_orders
```

Используются модели:

- `LogisticRegression`
- `RandomForestClassifier`

Split:

```text
time-based split
```

То есть:

- `train` — более ранние даты;
- `test` — более поздние даты.

Метрики:

- `ROC-AUC`
- `PR-AUC`
- `LogLoss`
- `Brier score`
- `Precision@10`
- `Recall@10`
- `Precision@20`
- `Recall@20`
- `Precision / Recall at threshold 0.5`

Артефакты:

```text
artifacts/models/lateness_model.joblib
artifacts/predictions/route_predictions.csv
artifacts/reports/lateness_model_metrics.json
```

Папка `artifacts/` не коммитится.

---

## 15. Prediction publishing

Файл:

```text
scripts/publish_route_predictions.py
```

Он берёт:

```text
artifacts/predictions/route_predictions.csv
```

и публикует predictions в:

- `PostgreSQL → route_predictions`
- `Redis → route:{route_id}:late_probability / route:{route_id}:prediction`
- `ClickHouse → route_prediction_events`

---

## 16. Dashboard tables

Файл:

```text
scripts/build_dashboard_tables.py
```

Создаёт таблицы:

### `daily_delivery_metrics`

Бизнес-метрики доставки по дням:

- `orders_count`
- `routes_count`
- `couriers_count`
- `avg_delay_minutes`
- `p50_delay_minutes`
- `p90_delay_minutes`
- `late_orders_count`
- `lateness_rate`

### `route_risk_summary`

Операционная таблица рискованных маршрутов:

- `route_id`
- `route_date`
- `zone_id`
- `weather`
- `vehicle_type`
- `num_orders`
- `target_has_late_orders`
- `target_lateness_rate`
- `predicted_late_probability`
- `risk_bucket`
- `prediction_result`

### `model_quality_by_day`

Мониторинг качества модели:

- `routes_count`
- `actual_bad_route_rate`
- `avg_predicted_late_probability`
- `brier_score`
- `true_positive`
- `false_positive`
- `false_negative`
- `true_negative`
- `precision_at_05`
- `recall_at_05`

---

## 17. Grafana

Grafana доступна:

```text
http://localhost:3000
```

Логин / пароль:

```text
admin / admin
```

Dashboard:

```text
Dashboards → Ozon Fresh → Ozon Delivery & ML Monitoring
```

Dashboard создаётся автоматически через provisioning.

Файлы:

```text
grafana/provisioning/datasources/postgres.yml
grafana/provisioning/dashboards/ozon-dashboard.yml
grafana/dashboards/ozon_delivery_ml_monitoring.json
scripts/create_grafana_dashboard.py
```

Панели:

- `Total orders`
- `Average lateness rate`
- `Average P90 delay minutes`
- `High-risk routes`
- `Orders count by day`
- `Lateness rate by day`
- `P90 delay minutes by day`
- `Precision and recall by day`
- `Routes by risk bucket`
- `Brier score by day`
- `Confusion matrix totals`
- `Top risky routes`

---

## 18. FastAPI

FastAPI доступен:

```text
http://localhost:8000
```

Swagger UI:

```text
http://localhost:8000/docs
```

Файл приложения:

```text
src/api/main.py
```

Dockerfile:

```text
Dockerfile.api
```

### API endpoints

#### Healthcheck

```http
GET /health
```

Проверка:

```bash
curl http://localhost:8000/health
```

Ожидаемо:

```json
{
  "postgres": true,
  "redis": true,
  "status": "ok"
}
```

#### Top risky routes

```http
GET /routes/top-risky?limit=10
```

Проверка:

```bash
curl "http://localhost:8000/routes/top-risky?limit=3"
```

#### Prediction by route_id

```http
GET /routes/{route_id}/prediction
```

Пример:

```bash
curl http://localhost:8000/routes/245/prediction
```

API сначала ищет prediction в Redis. Если в Redis нет ключа, идёт в PostgreSQL.

#### Summary metrics

```http
GET /metrics/summary
```

Проверка:

```bash
curl http://localhost:8000/metrics/summary
```

---

## 19. Как остановить проект

Остановить Docker-сервисы:

```bash
docker compose down
```

Важно не использовать без необходимости:

```bash
docker compose down -v
```

Потому что `-v` удалит Docker volumes с данными.

Остановить Airflow:

```text
Ctrl + C
```

в терминале, где запущен:

```bash
./scripts/start_airflow.sh
```

Если Airflow не остановился:

```bash
pkill -f airflow
```

---

## 20. Как продолжить работу после перезагрузки

Открыть Ubuntu / WSL:

```bash
wsl
```

Перейти в проект:

```bash
cd ~/projects/ozon-local
```

Запустить Docker-сервисы:

```bash
docker compose up -d
docker compose ps
```

Запустить Airflow:

```bash
./scripts/start_airflow.sh
```

Открыть:

```text
Airflow:  http://localhost:8080
Grafana:  http://localhost:3000
FastAPI:  http://localhost:8000/docs
Kafka UI: http://localhost:8082
```

---

## 21. Резюме для собеседования

Локальный production-like ML pipeline для задачи прогнозирования риска опоздания курьерских маршрутов.

Сгенерировал синтетические данные по заказам, маршрутам, курьерам и событиям доставки. Справочники и заказы загрузил в PostgreSQL, события — в ClickHouse, поток событий имитировал через Kafka, а актуальное состояние маршрутов и предсказания модели кэшировал в Redis.

На PySpark построил route-level feature table, обучил baseline-модель риска опоздания с time-based split и метриками ROC-AUC, PR-AUC, LogLoss, Brier score, Precision@K и Recall@K.

После обучения публикую предсказания обратно в PostgreSQL, Redis и ClickHouse. Для serving сделал FastAPI, для мониторинга — Grafana dashboard, а весь процесс оркестрировал через Airflow DAG.

---

## 22. Возможные улучшения

Следующие шаги развития проекта:

1. Добавить MLflow для model registry и tracking.
2. Добавить CatBoost / LightGBM.
3. Добавить real-time scoring consumer из Kafka.
4. Добавить drift monitoring.
5. Добавить алерты Grafana.
6. Добавить tests.
7. Добавить pre-commit / ruff / black.
8. Добавить CI.
9. Разнести dev/prod конфиги через `.env`.
10. Добавить Dockerfile для полного ML worker.

---

## 23. Git

Проверить статус:

```bash
git status
```

Проверить историю:

```bash
git log --oneline -10
```

В Git коммитятся:

- `src/`
- `scripts/`
- `dags/`
- `grafana/`
- `Dockerfile.api`
- `requirements-api.txt`
- `docker-compose.yml`
- `docker-compose.override.yml`
- `README.md`
- `.gitignore`

Не коммитятся:

- `.venv/`
- `.airflow-venv/`
- `airflow_home/`
- `data/`
- `artifacts/`
- `.env`
