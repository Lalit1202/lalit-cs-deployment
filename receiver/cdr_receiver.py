from kafka import KafkaConsumer
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

logging.info("Starting receiver...")

consumer = KafkaConsumer(
    'cdr-records',
    bootstrap_servers='kafka-service.kafka.svc.cluster.local:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='cdr-receiver-group-' + str(hash(__file__)),  # use unique group to reset offset
    auto_offset_reset='earliest',
    consumer_timeout_ms=15000  # will exit if no message in 15s
)

logging.info("Receiver started. Polling for messages...")

try:
    for msg in consumer:
        cdr = msg.value
        logging.info(f"[RECEIVED] {cdr['cdr_id']} | {cdr['caller']} ➜ {cdr['receiver']}")
except Exception as e:
    logging.error(f"Error while consuming messages: {e}")
finally:
    consumer.close()
    logging.info("Consumer closed.")
