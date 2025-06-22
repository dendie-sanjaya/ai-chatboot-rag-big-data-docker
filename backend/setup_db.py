# setup_db.py

import sqlite3
import os

# Path untuk database SQLite
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "ai-spark", "data", "raw", "device_info.db")

# Pastikan direktori 'raw' ada
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Hapus database lama jika ada
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)
    print(f"Database lama '{DB_PATH}' telah dihapus.")

# Buat koneksi ke database SQLite
# Jika database tidak ada, akan dibuat otomatis
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

print(f"Database SQLite '{DB_PATH}' berhasil dibuat.")

# Buat tabel device_info
create_table_sql = """
CREATE TABLE IF NOT EXISTS device_info (
    device_id TEXT PRIMARY KEY,
    device_model TEXT NOT NULL,
    location TEXT NOT NULL
);
"""
cursor.execute(create_table_sql)
print("Tabel 'device_info' berhasil dibuat.")

# Data untuk di-populate
data_to_insert = [
    ('DEV001', 'Router 001', 'Jakarta'),
    ('DEV002', 'Swicth 002', 'Bandung'),
    ('DEV003', 'Router 002', 'Surabaya'),
    ('DEV004', 'Router 005', 'Jakarta'),
    ('DEV005', 'Switch 004', 'Bandung')
]

# Masukkan data ke tabel
insert_sql = "INSERT INTO device_info (device_id, device_model, location) VALUES (?, ?, ?)"
cursor.executemany(insert_sql, data_to_insert)
conn.commit()

print(f"{len(data_to_insert)} baris data berhasil di-populate ke tabel 'device_info'.")

# Verifikasi data
print("\nVerifikasi data di tabel 'device_info':")
cursor.execute("SELECT * FROM device_info")
rows = cursor.fetchall()
for row in rows:
    print(row)

# Tutup koneksi
conn.close()
print("\nSetup database SQLite selesai.")