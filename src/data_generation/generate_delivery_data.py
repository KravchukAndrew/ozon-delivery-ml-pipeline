from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


SEED = 42

N_STORES = 5
N_COURIERS = 80
N_ROUTES = 300

MIN_ORDERS_PER_ROUTE = 4
MAX_ORDERS_PER_ROUTE = 14

BASE_DATE = "2026-06-01"
N_DAYS = 14

OUT_DIR = Path("data/raw")

MOSCOW_LAT = 55.751244
MOSCOW_LON = 37.618423


rng = np.random.default_rng(SEED)


def random_points_around(
    center_lat: float,
    center_lon: float,
    n: int,
    radius_km: float,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Генерирует n точек вокруг заданной широты/долготы.

    Это не дорожное расстояние, а географическая имитация.
    Для учебного проекта достаточно.
    """
    angles = rng.uniform(0, 2 * np.pi, n)
    radii = radius_km * np.sqrt(rng.uniform(0, 1, n))

    delta_lat = (radii * np.cos(angles)) / 111.0
    delta_lon = (radii * np.sin(angles)) / (
        111.0 * np.cos(np.deg2rad(center_lat))
    )

    return center_lat + delta_lat, center_lon + delta_lon


def haversine_km(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    """
    Расстояние между двумя точками на сфере.

    В реальной логистике лучше использовать дорожное расстояние / ETA
    из картографического сервиса, но для учебного проекта подходит.
    """
    earth_radius_km = 6371.0

    phi1 = np.deg2rad(lat1)
    phi2 = np.deg2rad(lat2)
    d_phi = np.deg2rad(lat2 - lat1)
    d_lambda = np.deg2rad(lon2 - lon1)

    a = (
        np.sin(d_phi / 2) ** 2
        + np.cos(phi1) * np.cos(phi2) * np.sin(d_lambda / 2) ** 2
    )
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    return float(earth_radius_km * c)


def make_stores() -> pd.DataFrame:
    zones = ["north", "south", "east", "west", "center"]

    store_lats, store_lons = random_points_around(
        MOSCOW_LAT,
        MOSCOW_LON,
        N_STORES,
        radius_km=9.0,
    )

    rows = []
    for i in range(N_STORES):
        rows.append(
            {
                "store_id": i + 1,
                "store_name": f"darkstore_{i + 1}",
                "zone_id": zones[i % len(zones)],
                "lat": round(float(store_lats[i]), 6),
                "lon": round(float(store_lons[i]), 6),
            }
        )

    return pd.DataFrame(rows)


def make_couriers() -> pd.DataFrame:
    vehicle_types = np.array(["bike", "car", "foot"])
    vehicle_probs = np.array([0.35, 0.45, 0.20])

    rows = []
    for courier_id in range(1, N_COURIERS + 1):
        vehicle_type = str(rng.choice(vehicle_types, p=vehicle_probs))

        if vehicle_type == "car":
            capacity_orders = int(rng.integers(10, 18))
            base_speed_kmh = float(rng.normal(28, 5))
        elif vehicle_type == "bike":
            capacity_orders = int(rng.integers(6, 11))
            base_speed_kmh = float(rng.normal(18, 3))
        else:
            capacity_orders = int(rng.integers(3, 7))
            base_speed_kmh = float(rng.normal(6, 1))

        experience_days = int(rng.integers(10, 900))

        # Условная историческая склонность к опозданиям.
        # У новичков она в среднем выше.
        base_late_rate = float(rng.beta(2, 12))
        if experience_days < 60:
            base_late_rate += 0.08

        rows.append(
            {
                "courier_id": courier_id,
                "vehicle_type": vehicle_type,
                "capacity_orders": capacity_orders,
                "base_speed_kmh": round(max(base_speed_kmh, 3.0), 2),
                "experience_days": experience_days,
                "historical_lateness_rate": round(min(base_late_rate, 0.65), 4),
            }
        )

    return pd.DataFrame(rows)


def choose_route_start_at() -> pd.Timestamp:
    base = pd.Timestamp(BASE_DATE)
    day_offset = int(rng.integers(0, N_DAYS))

    # Типичные окна начала маршрутов.
    hour = int(rng.choice([9, 10, 11, 12, 14, 16, 18, 20]))
    minute = int(rng.choice([0, 15, 30, 45]))

    return base + pd.Timedelta(days=day_offset, hours=hour, minutes=minute)


def weather_for_day() -> str:
    return str(rng.choice(["clear", "rain", "heavy_rain"], p=[0.72, 0.22, 0.06]))


def weather_speed_factor(weather: str) -> float:
    if weather == "clear":
        return 1.0
    if weather == "rain":
        return 0.85
    return 0.70


def make_routes_orders_events(
    stores_df: pd.DataFrame,
    couriers_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    route_rows = []
    order_rows = []
    event_rows = []

    order_id = 1
    event_id = 1

    for route_id in range(1, N_ROUTES + 1):
        store = stores_df.sample(1, random_state=int(rng.integers(0, 1_000_000))).iloc[0]
        courier = couriers_df.sample(1, random_state=int(rng.integers(0, 1_000_000))).iloc[0]

        planned_start_at = choose_route_start_at()
        route_date = planned_start_at.date()
        day_of_week = planned_start_at.day_name()
        hour = planned_start_at.hour

        weather = weather_for_day()
        is_rush_hour = hour in {9, 10, 18, 19, 20}

        n_orders = int(rng.integers(MIN_ORDERS_PER_ROUTE, MAX_ORDERS_PER_ROUTE + 1))

        # Радиус зависит от транспорта: пешие маршруты компактнее, машины шире.
        if courier["vehicle_type"] == "car":
            radius_km = 7.0
        elif courier["vehicle_type"] == "bike":
            radius_km = 4.0
        else:
            radius_km = 1.8

        order_lats, order_lons = random_points_around(
            float(store["lat"]),
            float(store["lon"]),
            n_orders,
            radius_km=radius_km,
        )

        speed = (
            float(courier["base_speed_kmh"])
            * weather_speed_factor(weather)
            * (0.78 if is_rush_hour else 1.0)
        )
        speed = max(speed, 3.0)

        planned_elapsed_min = 0.0
        actual_elapsed_min = 0.0
        total_distance_km = 0.0

        prev_lat = float(store["lat"])
        prev_lon = float(store["lon"])

        route_late_count = 0
        route_total_delay = 0.0

        for stop_number in range(1, n_orders + 1):
            lat = float(order_lats[stop_number - 1])
            lon = float(order_lons[stop_number - 1])

            distance_to_prev_km = haversine_km(prev_lat, prev_lon, lat, lon)
            total_distance_km += distance_to_prev_km

            service_time_min = max(float(rng.normal(5.5, 1.2)), 2.0)

            # План часто чуть оптимистичнее факта.
            planned_travel_min = distance_to_prev_km / max(speed * 1.08, 1.0) * 60.0
            planned_increment_min = planned_travel_min + service_time_min

            # Фактическая задержка зависит от погоды, часа пик, загруженности и курьера.
            weather_delay = {"clear": 0.0, "rain": 2.0, "heavy_rain": 5.0}[weather]
            rush_delay = 2.5 if is_rush_hour else 0.0
            load_delay = max(n_orders - 9, 0) * 0.7
            courier_delay = float(courier["historical_lateness_rate"]) * 12.0
            random_delay = float(rng.normal(0.5, 2.5))

            actual_increment_min = (
                planned_increment_min
                + weather_delay
                + rush_delay
                + load_delay
                + courier_delay
                + random_delay
            )
            actual_increment_min = max(actual_increment_min, 1.0)

            planned_elapsed_min += planned_increment_min
            actual_elapsed_min += actual_increment_min

            planned_arrival_at = planned_start_at + pd.Timedelta(minutes=planned_elapsed_min)

            # Promise time — клиентское обещание. Иногда система даёт небольшой буфер.
            promise_buffer_min = max(float(rng.normal(6.0, 4.0)), -2.0)
            promised_delivery_at = planned_arrival_at + pd.Timedelta(minutes=promise_buffer_min)

            actual_delivery_at = planned_start_at + pd.Timedelta(minutes=actual_elapsed_min)
            delay_minutes = (
                actual_delivery_at - promised_delivery_at
            ).total_seconds() / 60.0

            is_late = int(delay_minutes > 0)
            if is_late:
                route_late_count += 1
                route_total_delay += delay_minutes

            order_created_at = planned_start_at - pd.Timedelta(
                minutes=int(rng.integers(40, 220))
            )

            item_count = int(rng.integers(1, 9))
            weight_kg = round(float(rng.gamma(2.0, 2.0)), 2)
            volume_l = round(weight_kg * float(rng.uniform(1.2, 3.5)), 2)

            order_rows.append(
                {
                    "order_id": order_id,
                    "route_id": route_id,
                    "courier_id": int(courier["courier_id"]),
                    "store_id": int(store["store_id"]),
                    "zone_id": str(store["zone_id"]),
                    "order_created_at": order_created_at,
                    "planned_start_at": planned_start_at,
                    "planned_arrival_at": planned_arrival_at,
                    "promised_delivery_at": promised_delivery_at,
                    "actual_delivery_at": actual_delivery_at,
                    "lat": round(lat, 6),
                    "lon": round(lon, 6),
                    "stop_number": stop_number,
                    "item_count": item_count,
                    "weight_kg": weight_kg,
                    "volume_l": volume_l,
                    "distance_to_prev_km": round(distance_to_prev_km, 3),
                    "delay_minutes": round(delay_minutes, 2),
                    "is_late": is_late,
                }
            )

            events_for_order = [
                ("created", order_created_at, float(store["lat"]), float(store["lon"])),
                ("assigned", planned_start_at - pd.Timedelta(minutes=15), float(store["lat"]), float(store["lon"])),
                ("arrived_to_customer", actual_delivery_at - pd.Timedelta(minutes=service_time_min), lat, lon),
                ("delivered", actual_delivery_at, lat, lon),
            ]

            for event_type, event_time, event_lat, event_lon in events_for_order:
                event_rows.append(
                    {
                        "event_id": event_id,
                        "event_time": event_time,
                        "order_id": order_id,
                        "courier_id": int(courier["courier_id"]),
                        "route_id": route_id,
                        "event_type": event_type,
                        "lat": round(event_lat, 6),
                        "lon": round(event_lon, 6),
                    }
                )
                event_id += 1

            prev_lat = lat
            prev_lon = lon
            order_id += 1

        route_rows.append(
            {
                "route_id": route_id,
                "store_id": int(store["store_id"]),
                "courier_id": int(courier["courier_id"]),
                "zone_id": str(store["zone_id"]),
                "route_date": route_date,
                "planned_start_at": planned_start_at,
                "day_of_week": day_of_week,
                "start_hour": hour,
                "weather": weather,
                "is_rush_hour": int(is_rush_hour),
                "num_orders": n_orders,
                "total_distance_km": round(total_distance_km, 3),
                "planned_duration_min": round(planned_elapsed_min, 2),
                "actual_duration_min": round(actual_elapsed_min, 2),
                "late_orders": route_late_count,
                "lateness_rate": round(route_late_count / n_orders, 4),
                "total_delay_minutes": round(route_total_delay, 2),
            }
        )

    return (
        pd.DataFrame(route_rows),
        pd.DataFrame(order_rows),
        pd.DataFrame(event_rows),
    )


def save_outputs(
    stores_df: pd.DataFrame,
    couriers_df: pd.DataFrame,
    routes_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    events_df: pd.DataFrame,
) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    stores_df.to_csv(OUT_DIR / "stores.csv", index=False)
    couriers_df.to_csv(OUT_DIR / "couriers.csv", index=False)
    routes_df.to_csv(OUT_DIR / "routes.csv", index=False)
    orders_df.to_csv(OUT_DIR / "orders.csv", index=False)
    events_df.to_csv(OUT_DIR / "delivery_events.csv", index=False)

    # Parquet тоже сохраняем: это ближе к Data Lake / Hadoop / Spark-практике.
    stores_df.to_parquet(OUT_DIR / "stores.parquet", index=False)
    couriers_df.to_parquet(OUT_DIR / "couriers.parquet", index=False)
    routes_df.to_parquet(OUT_DIR / "routes.parquet", index=False)
    orders_df.to_parquet(OUT_DIR / "orders.parquet", index=False)
    events_df.to_parquet(OUT_DIR / "delivery_events.parquet", index=False)


def main() -> None:
    stores_df = make_stores()
    couriers_df = make_couriers()
    routes_df, orders_df, events_df = make_routes_orders_events(
        stores_df=stores_df,
        couriers_df=couriers_df,
    )

    save_outputs(
        stores_df=stores_df,
        couriers_df=couriers_df,
        routes_df=routes_df,
        orders_df=orders_df,
        events_df=events_df,
    )

    print("Synthetic delivery data generated successfully.")
    print(f"Output directory: {OUT_DIR.resolve()}")
    print()
    print(f"stores:          {stores_df.shape}")
    print(f"couriers:        {couriers_df.shape}")
    print(f"routes:          {routes_df.shape}")
    print(f"orders:          {orders_df.shape}")
    print(f"delivery_events: {events_df.shape}")
    print()
    print("Order lateness rate:", round(float(orders_df["is_late"].mean()), 4))
    print("Route lateness rate:", round(float((routes_df["late_orders"] > 0).mean()), 4))
    print()
    print("Sample orders:")
    print(orders_df.head(5).to_string(index=False))


if __name__ == "__main__":
    main()