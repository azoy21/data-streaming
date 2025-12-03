from kafka import KafkaProducer
import json
import time
import random

def produce_data():
    producer = KafkaProducer(
        bootstrap_servers=['kafka:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    while True:
        data = {
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "value": round(random.uniform(10.0, 50.0), 2)
        }
        producer.send('data_topic', data)
        print(f"Sent: {data}")
        time.sleep(2)

if __name__ == "__main__":
    time.sleep(15)  # Wait for Kafka to be ready
    produce_data()
