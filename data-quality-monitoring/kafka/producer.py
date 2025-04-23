
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def send_quality_metrics(metric):
    producer.send('data-quality-metrics', metric)

if __name__ == "__main__":
    while True:
        metric = {
            "pipeline_id": "pipeline_1",
            "null_percentage": 0.05,
            "record_count": 20000,
            "timestamp": time.time()
        }
        send_quality_metrics(metric)
        time.sleep(10)
