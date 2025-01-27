import threading
from db_connection import connect_to_postgres
from kafka_connection import setup_kafka_producer, setup_kafka_consumer
import json

# Producer function
def producer_thread():
    conn, cur = connect_to_postgres("192.168.242.8", "postgres", "postgres", "password")
    if conn is None:
        return

    producer = setup_kafka_producer()
    
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

# Consumer function
def consumer_thread():
    conn, cur = connect_to_postgres("localhost", "mydatabase", "admin", "admin123")
    if conn is None:
        return

    consumer = setup_kafka_consumer()
    
    for message in consumer:
        data = message.value
        npm = data.get('npm')
        nama = data.get('nama')
        
        print(f"Data diterima dari Kafka: {data}")

        # Insert or update data di table siswa
        try:
            print("Menjalankan query INSERT/UPDATE di tabel siswa")
            cur.execute("""
                INSERT INTO siswa (npm, nama) VALUES (%s, %s)
                ON CONFLICT (npm) DO UPDATE SET nama = EXCLUDED.nama
            """, (npm, nama))
            conn.commit()
            print(f"Data diperbarui di tabel siswa: {data}")

            # Cek apakah NPM ganjil
            if int(npm) % 2 != 0:
                print("NPM ganjil, menambahkan ke tabel siswa_new")
                cur.execute("""
                    INSERT INTO siswa_new (npm, nama) VALUES (%s, %s)
                    ON CONFLICT (npm) DO UPDATE SET nama = EXCLUDED.nama
                """, (npm, nama))
                conn.commit()
                print(f"Data diperbarui di tabel siswa_new: {data}")

            # Cek apakah NPM lebih dari >= 20
            if int(npm) >= 20:
                print("NPM lebih dari 20, menambahkan ke tabel siswa_new_20")
                cur.execute("""
                    INSERT INTO siswa_new_20 (npm, nama) VALUES (%s, %s)
                    ON CONFLICT (npm) DO UPDATE SET nama = EXCLUDED.nama
                """, (npm, nama))
                conn.commit()
                print(f"Data diperbarui di tabel siswa_new_20: {data}")

        except Exception as e:
            print("Error saat memasukkan data ke database:", e)
            # Rollback transaksi saat terjadi error
            conn.rollback()

# Menjalankan producer dan consumer dalam thread yang berbeda
if __name__ == "__main__":
    producer_thread = threading.Thread(target=producer_thread)
    consumer_thread = threading.Thread(target=consumer_thread)

    producer_thread.start()
    consumer_thread.start()

    producer_thread.join()
    consumer_thread.join()
