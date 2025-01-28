# kafka_connection.py
from kafka import KafkaProducer
import json

# Fungsi untuk membuat koneksi Kafka Producer
def get_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',  # Ganti dengan alamat server Kafka Anda
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Kafka connection successful!")
        return producer
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        return None

# # Test koneksi ke Kafka
# if __name__ == "__main__":
#     get_kafka_producer()
