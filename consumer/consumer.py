from kafka import KafkaConsumer
import json
import psycopg2
import time

def create_table():
    conn = psycopg2.connect(
        host="postgres",
        database="datastream",
        user="user",
        password="password"
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS data (
            id SERIAL PRIMARY KEY,
            timestamp TEXT,
            value NUMERIC
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

def consume_and_store():
    consumer = KafkaConsumer(
        'data_topic',
        bootstrap_servers=['kafka:29092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='data_consumer',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    conn = psycopg2.connect(
        host="postgres",
        database="datastream",
        user="user",
        password="password"
    )
    cur = conn.cursor()

    for message in consumer:
        data = message.value
        print(f"Received: {data}")
        cur.execute("INSERT INTO data (timestamp, value) VALUES (%s, %s)", (data['timestamp'], data['value']))
        conn.commit()

    cur.close()
    conn.close()

if __name__ == "__main__":
    time.sleep(10)  # Wait for Kafka and Postgres to be ready
    create_table()
    consume_and_store()
