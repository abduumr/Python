import requests
import json
from kafka import KafkaProducer
import time

# Fungsi untuk mengambil data pengguna acak dari randomuser.me API
def fetch_random_user_data():
    url = "https://randomuser.me/api/?results=1"  # Mengambil 1 data pengguna acak
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        return data['results'][0]  # Mengambil data pengguna pertama
    else:
        print("Gagal mengambil data dari API.")
        return None

# Fungsi untuk mengirim data ke Kafka
def send_to_kafka(data):
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',  # Ganti dengan server Kafka Anda
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send('api-dev', value=data)  # Mengirimkan data ke topic api-dev
    producer.flush()
    print(f"Data {data} berhasil dikirim ke Kafka.")

# Producer berjalan terus menerus dengan interval 30 detik
while True:
    user_data = fetch_random_user_data()
    if user_data:
        send_to_kafka(user_data)
    time.sleep(2)  # Menunggu selama 30 detik sebelum mengambil data berikutnya
