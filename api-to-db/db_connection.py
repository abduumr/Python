import psycopg2

# Fungsi untuk koneksi ke database utama
def connect_to_main_db():
    try:
        conn = psycopg2.connect(
            host="192.168.242.8",
            database="postgres",
            user="postgres",
            password="password"
        )
        print("Terhubung ke database utama.")
        return conn
    except Exception as e:
        print("Gagal terhubung ke database utama:", e)
        return None

# Fungsi untuk koneksi ke database Australia
def connect_to_australia_db():
    try:
        conn_aus = psycopg2.connect(
            host="localhost",
            database="mydatabase",
            user="admin",
            password="admin123"
        )
        print("Terhubung ke database Australia.")
        return conn_aus
    except Exception as e:
        print("Gagal terhubung ke database Australia:", e)
        return None
