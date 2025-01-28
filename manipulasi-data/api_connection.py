# api_connection.py
import requests

# Fungsi untuk mengambil data dari API
def fetch_data_from_api():
    try:
        print("Fetching data from API...")
        url = "https://randomuser.me/api/?results=10"  # Ambil 10 data dari API
        response = requests.get(url)
        data = response.json()['results']
        return data
    except Exception as e:
        print(f"Error fetching data from API: {e}")
        return None
