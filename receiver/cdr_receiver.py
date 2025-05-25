from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'cdr-records',
    bootstrap_servers='kafka-service.kafka.svc.cluster.local:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='cdr-receiver-group',
    auto_offset_reset='earliest'
)

print("Receiver started. Listening to cdr-records topic...", flush=True)

for msg in consumer:
    cdr = msg.value
    print(f"[RECEIVED] {cdr['cdr_id']} | {cdr['caller']} ➜ {cdr['receiver']}", flush=True)
