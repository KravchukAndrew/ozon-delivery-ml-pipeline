import json
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "127.0.0.1:9092"
})

event = {
    "order_id": 10,
    "route_id": 1001,
    "courier_id": 101,
    "status": "on_the_way",
}

producer.produce(
    "order_status",
    json.dumps(event).encode("utf-8"),
)

producer.flush()

print("sent:", event)
