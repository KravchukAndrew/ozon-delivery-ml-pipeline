from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "127.0.0.1:9092",
    "group.id": "ozon-local-consumer",
    "auto.offset.reset": "earliest",
})

consumer.subscribe(["order_status"])

print("Waiting for message...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("ERROR:", msg.error())
            continue

        print("received:", msg.value().decode("utf-8"))
        break

finally:
    consumer.close()
