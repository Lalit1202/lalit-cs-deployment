from kafka import KafkaProducer
import json, time, uuid, random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='kafka-service.kafka.svc.cluster.local:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_cdr():
    return {
        "cdr_id": str(uuid.uuid4()),
        "caller": f"+91{random.randint(6000000000, 9999999999)}",
        "receiver": f"+91{random.randint(6000000000, 9999999999)}",
        "start_time": datetime.utcnow().isoformat(),
        "duration_sec": random.randint(30, 600),
        "call_type": random.choice(["local", "national", "international"]),
        "status": random.choice(["completed", "missed", "dropped"]),
    }

while True:
    cdr = generate_cdr()
    producer.send("cdr-records", cdr)
    print(f"[SENT] {cdr['cdr_id']}", flush=True)
    time.sleep(1)
