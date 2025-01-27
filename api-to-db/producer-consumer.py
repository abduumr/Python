import requests
import json
import time
from kafka import KafkaProducer, KafkaConsumer
import threading
import psycopg2
from db_connection import connect_to_main_db, connect_to_australia_db


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

# Fungsi untuk consumer Kafka
def consume_from_kafka():
    conn = connect_to_main_db()
    if not conn:
        exit(1)

    cur = conn.cursor()

    # Setup Kafka consumer
    consumer = KafkaConsumer(
        'api-dev',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("Kafka consumer siap.")

    # Proses data dari Kafka
    for message in consumer:
        data = message.value
        print(f"Data diterima dari Kafka: {data}")

        try:
            # Ekstrak data pengguna
            first_name = str(data.get('name', {}).get('first', '') or '')
            last_name = str(data.get('name', {}).get('last', '') or '')
            gender = str(data.get('gender', '') or '')
            email = str(data.get('email', '') or '')
            city = str(data.get('location', {}).get('city', '') or '')
            country = str(data.get('location', {}).get('country', '') or '')
            age = int(data.get('dob', {}).get('age', 0) or 0)
            phone = str(data.get('phone', '') or '')

            # Insert data ke tabel anggota
            cur.execute("""
                INSERT INTO staging.anggota (first_name, last_name, gender, email, city, country, age, phone)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            """, (first_name, last_name, gender, email, city, country, age, phone))
            
            id = cur.fetchone()[0]
            conn.commit()
            print(f"Data berhasil dimasukkan ke tabel anggota dengan id: {id}")

            # Memasukkan data pria ke tabel anggota_pria
            if gender == 'male':
                print("Gender adalah 'male', memasukkan data ke tabel anggota_pria.")
                cur.execute("""
                    INSERT INTO staging.anggota_pria (id, first_name, last_name, gender, email, city, country, age, phone)
                    SELECT id, first_name, last_name, gender, email, city, country, age, phone
                    FROM staging.anggota
                    WHERE id = %s
                    ON CONFLICT (id) DO NOTHING;
                """, (id,))
                conn.commit()

            # Memasukkan data ke tabel anggota_anti_australia
            if country != 'Australia':
                print("Memasukkan data ke tabel anggota_anti_australia.")
                cur.execute("""
                    INSERT INTO staging.anggota_anti_australia (id, first_name, last_name, gender, email, city, country, age, phone)
                    SELECT id, first_name, last_name, gender, email, city, country, age, phone
                    FROM staging.anggota
                    WHERE id = %s
                    ON CONFLICT (id) DO NOTHING;
                """, (id,))
                conn.commit()

            # Memasukkan data dengan country = Australia ke dalam database lain
            if country == 'Australia':
                conn_aus = connect_to_australia_db()
                if conn_aus:
                    cur_aus = conn_aus.cursor()
                    print("Memasukkan data ke tabel anggota_australia.")
                    cur_aus.execute("""
                        INSERT INTO staging.anggota_australia (id, first_name, last_name, gender, email, city, country, age, phone)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING;
                    """, (id, first_name, last_name, gender, email, city, country, age, phone))
                    conn_aus.commit()
                    cur_aus.close()
                    conn_aus.close()

        except Exception as e:
            print("Error saat memasukkan data ke database:", e)
            conn.rollback()

# Fungsi untuk producer Kafka
def produce_to_kafka():
    while True:
        user_data = fetch_random_user_data()
        if user_data:
            send_to_kafka(user_data)
        time.sleep(2)  # Menunggu selama 2 detik sebelum mengambil data berikutnya

# Menjalankan producer dan consumer dalam thread yang berbeda
if __name__ == "__main__":
    producer_thread = threading.Thread(target=produce_to_kafka)
    consumer_thread = threading.Thread(target=consume_from_kafka)

    producer_thread.start()
    consumer_thread.start()

    producer_thread.join()
    consumer_thread.join()
