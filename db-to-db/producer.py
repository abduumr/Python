import psycopg2
import json
from kafka import KafkaProducer

# Koneksi ke PostgreSQL (database sumber)
conn = psycopg2.connect(
    host="192.168.242.8",
    database="postgres",
    user="postgres",
    password="password"
)
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()

# Setup Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def listen_for_changes():
    cur.execute("LISTEN kafka_channel;")
    print("Listening for changes on kafka_channel...")
    while True:
        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            data = json.loads(notify.payload)
            producer.send('siswa_topic', data)
            print(f"Data dikirim ke Kafka: {data}")

listen_for_changes()
