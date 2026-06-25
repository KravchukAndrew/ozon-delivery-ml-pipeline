import clickhouse_connect

client = clickhouse_connect.get_client(
    host="127.0.0.1",
    port=8123,
    username="default",
    password="ozon",
    database="ozon",
)

result = client.query("""
    SELECT
        route_id,
        event_type,
        count()
    FROM delivery_events
    GROUP BY route_id, event_type
    ORDER BY route_id, event_type
""")

for row in result.result_rows:
    print(row)
