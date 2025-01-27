# db_connection.py
import psycopg2

def connect_to_postgres(host, database, user, password):
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        return conn, cur
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None, None
