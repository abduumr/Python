import psycopg2
from psycopg2 import OperationalError

def check_postgres_connection():
    # Konfigurasi koneksi PostgreSQL
    host = "localhost"
    user = "admin"
    password = "admin123"
    database = "mydatabase"

    try:
        # Mencoba koneksi ke database PostgreSQL
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        
        # Jika berhasil
        print("Koneksi ke PostgreSQL berhasil!")
        
    except OperationalError as e:
        # Jika terjadi error saat koneksi
        print(f"Error: Tidak dapat terhubung ke PostgreSQL: {e}")

    finally:
        # Tutup koneksi jika berhasil terhubung
        if 'connection' in locals() and connection:
            connection.close()
            print("Koneksi ditutup.")

if __name__ == "__main__":
    check_postgres_connection()
