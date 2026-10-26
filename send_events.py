"""Usage: python scripts/send_events.py <API_URL> [count]"""
import json, random, sys, urllib.request

url = sys.argv[1]
count = int(sys.argv[2]) if len(sys.argv) > 2 else 20
types = ["order_created", "payment_failed", "user_signup", "error"]

for i in range(count):
    body = {"event_type": random.choice(types), "payload": {"amount": round(random.uniform(5, 500), 2), "n": i}}
    req = urllib.request.Request(url, json.dumps(body).encode(), {"Content-Type": "application/json"})
    print(urllib.request.urlopen(req).read().decode())
