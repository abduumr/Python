import requests
import pandas as pd

# URL API
url = "https://api-sscasn.bkn.go.id/2024/portal/spf"

# Header untuk permintaan
headers = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Origin": "https://sscasn.bkn.go.id",
    "Referer": "https://sscasn.bkn.go.id/"
}

# Fungsi untuk mengambil data dari satu halaman
def fetch_data(offset):
    params = {
        "kode_ref_pend": "5191141",
        "pengadaan_kd": "2",
        "offset": offset
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Permintaan gagal pada offset {offset} dengan status kode: {response.status_code}")
        return None

# Mengambil data dari semua halaman
all_data = []
offset = 0
data_per_page = 10  # API mengembalikan 100 data per halaman
total_data = 869  # Total data berdasarkan metadata API

while offset < total_data:
    print(f"Mengambil data dengan offset {offset}...")
    json_data = fetch_data(offset)
    if json_data:
        # Akses data utama
        main_data = json_data.get("data", {}).get("data", [])
        all_data.extend(main_data)  # Tambahkan data ke daftar utama
    offset += data_per_page

# Konversi data ke DataFrame dan simpan ke CSV
if all_data:
    df = pd.DataFrame(all_data)
    csv_filename = "data_sscasn_all_data.csv"
    df.to_csv(csv_filename, index=False)
    print(f"Semua data berhasil disimpan ke '{csv_filename}'")
else:
    print("Tidak ada data yang berhasil diambil.")
