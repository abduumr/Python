# kafka_connection.py
from kafka import KafkaProducer, KafkaConsumer
import json

def setup_kafka_producer():
    return KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def setup_kafka_consumer():
    return KafkaConsumer(
        'siswa_topic',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
