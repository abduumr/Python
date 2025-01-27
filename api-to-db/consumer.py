import json
from kafka import KafkaConsumer
from db_connection import connect_to_main_db, connect_to_australia_db

# jika anggotanya ber gender = male , maka akan ditambahkan juga kedalam table anggota_pria, btw karena id itu dibuat dengan auto-inceremt dari db , jadi kita membuat variable id untuk dimasukan kedalam db
# data anggota selain australia akan ditambahkan di table_anti_australia
# jika ada aggonta yang countrya australia akan disimpan di db lain
# Memecah koneksi db di file connection.py dan memanggilnya

# Koneksi ke database utama
conn = connect_to_main_db()
if not conn:
    exit(1)

cur = conn.cursor()

# Setup Kafka consumer
try:
    consumer = KafkaConsumer(
        'api-dev',
        bootstrap_servers='192.168.242.86:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("Kafka consumer siap.")
except Exception as e:
    print("Gagal terhubung ke Kafka:", e)
    exit(1)

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
