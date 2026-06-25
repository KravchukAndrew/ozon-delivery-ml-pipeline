from sqlalchemy import create_engine, text

engine = create_engine(
    "postgresql+psycopg2://ozon:ozon@127.0.0.1:5432/ozon"
)

with engine.connect() as conn:
    result = conn.execute(text("SELECT count(*) FROM orders"))
    print("orders count:", result.scalar())

    rows = conn.execute(text("""
        SELECT
            order_id,
            courier_id,
            route_id,
            delivered_at > promised_at AS is_late
        FROM orders
        ORDER BY order_id
    """))

    for row in rows:
        print(row)
