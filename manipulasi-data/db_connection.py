# db_connection.py
import psycopg2

# Koneksi ke Database Sumber
SOURCE_DB = {
    "dbname": "mydatabase",
    "user": "admin",
    "password": "admin123",
    "host": "localhost",
    "port": 5432
}

# Koneksi ke Database Target
TARGET_DB = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "password",
    "host": "192.168.242.8",
    "port": 5432
}

# Fungsi untuk mendapatkan koneksi ke database sumber
def get_source_connection():
    try:
        conn = psycopg2.connect(**SOURCE_DB)
        print("Connection to source database successful!")
        return conn
    except Exception as e:
        print(f"Error connecting to source DB: {e}")
        return None

# Fungsi untuk mendapatkan koneksi ke database target
def get_target_connection():
    try:
        conn = psycopg2.connect(**TARGET_DB)
        print("Connection to target database successful!")
        return conn
    except Exception as e:
        print(f"Error connecting to target DB: {e}")
        return None

# # Test koneksi ke database
# if __name__ == "__main__":
#     print("Testing source DB connection...")
#     get_source_connection()

#     print("Testing target DB connection...")
#     get_target_connection()
