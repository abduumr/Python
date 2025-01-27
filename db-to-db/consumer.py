from kafka import KafkaConsumer
import json
import psycopg2

#memasukan data yang berada di producer ke consumer dan jika npm-nya ganjil maka juga dimasukan kedalam table siswa_new
# Koneksi ke PostgreSQL (database target)
try:
    conn = psycopg2.connect(
        host="localhost",
        database="mydatabase",
        user="admin",
        password="admin123"
    )
    cur = conn.cursor()
    print("Terhubung ke database target.")
except Exception as e:
    print("Gagal terhubung ke database:", e)

# Setup Kafka consumer
try:
    consumer = KafkaConsumer(
        'siswa_topic',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("Kafka consumer siap.")
except Exception as e:
    print("Gagal terhubung ke Kafka:", e)

# Proses data dari Kafka
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