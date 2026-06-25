import redis

r = redis.Redis(
    host="127.0.0.1",
    port=6379,
    decode_responses=True,
)

r.set("route:1001:score", "0.87")
r.set("route:1002:score", "0.91")

print("route:1001:score =", r.get("route:1001:score"))
print("route:1002:score =", r.get("route:1002:score"))
