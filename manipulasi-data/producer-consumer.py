import time
from kafka_connection import get_kafka_producer
from db_connection import get_source_connection, get_target_connection
from api_connection import fetch_data_from_api

# Fungsi untuk ambil kode dari database berdasarkan huruf pertama nama depan
def fetch_code_from_db(first_name, last_name):
    try:
        name_combined = first_name[0]  # Ambil huruf pertama dari first_name
        conn = get_source_connection()
        if conn is None:
            return '0'  # Kembalikan kode default '0' jika tidak ada koneksi
        cur = conn.cursor()
        query = """
            SELECT code
            FROM public.code
            WHERE code_name = %s
        """
        cur.execute(query, (name_combined,))
        result = cur.fetchone()
        cur.close()
        conn.close()

        if result:
            return result[0]
        else:
            print(f"No matching code for: {name_combined}")
            return '0'  # Kembalikan '0' jika tidak ada kode yang cocok
    except Exception as e:
        print(f"Error fetching code from DB: {e}")
        return '0'  # Jika terjadi error, kembalikan '0'

# Fungsi untuk menyimpan data ke database target
def save_to_target_db(record):
    try:
        print(f"Saving to DB: {record}")
        conn = get_target_connection()
        if conn is None:
            return
        cur = conn.cursor()
        
        # Pastikan data yang akan dimasukkan sesuai dengan batasan kolom
        full_name = f"{record['first_name']} {record['last_name']}"  # Gabungkan first_name dan last_name
        if len(full_name) > 100:
            print(f"Name too long, truncating to 100 characters: {full_name[:100]}")
            full_name = full_name[:100]  # Potong jika terlalu panjang

        if len(record['code']) > 4:
            print(f"Code too long, truncating to 4 characters: {record['code'][:4]}")
            record['code'] = record['code'][:4]  # Potong jika terlalu panjang
        
        query = """
            INSERT INTO public.name_code (name, code)
            VALUES (%s, %s)
        """
        cur.execute(query, (full_name, record['code']))
        conn.commit()
        cur.close()
        conn.close()
        print(f"Data saved to target DB: {full_name} with code {record['code']}")
    except Exception as e:
        print(f"Error saving data to target DB: {e}")

# Fungsi untuk mengambil data dari API dan mengirimkannya ke Kafka
def consume_data():
    try:
        processed_count = 0  # Variabel untuk menghitung jumlah data yang diproses
        
        while True:  # Loop terus menerus untuk streaming
            data = fetch_data_from_api()  # Ambil data dari API
            
            if not data:  # Jika tidak ada data, berhenti sejenak
                print("No data fetched, retrying...")
                time.sleep(5)  # Tunggu 5 detik sebelum mencoba lagi
                continue  # Lanjutkan ke iterasi berikutnya
            
            for user in data:
                first_name = user['name']['first']
                last_name = user['name']['last']
                
                # Panggil fungsi untuk ambil kode dari DB
                code = fetch_code_from_db(first_name, last_name)
                
                # Jika tidak ada kode yang ditemukan, kode akan tetap '0'
                record = {'first_name': first_name, 'last_name': last_name, 'code': code}
                
                # Kirim data ke Kafka
                producer.send('user_data', value=record)  # Kirim ke topik Kafka
                print(f"Produced: {record}")
                
                # Simpan ke database target
                save_to_target_db(record)
                processed_count += 1
            
            print(f"Total records processed: {processed_count}")
            time.sleep(1)  # Tunggu 1 detik sebelum mengambil data lagi
    except Exception as e:
        print(f"Error in consume_data: {e}")

# Inisialisasi Kafka producer
producer = get_kafka_producer()

if __name__ == "__main__":
    consume_data()
