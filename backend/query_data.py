# scripts/query_data.py

from pyspark.sql import SparkSession
import os

# --- 1. Inisialisasi SparkSession ---
# Menggunakan mode lokal, sama seperti ETL.
# Untuk production, ini juga perlu diganti ke master("yarn"), master("k8s://..."), dst.
spark = SparkSession.builder \
    .appName("DeviceActivityQuery") \
    .master("local[*]") \
    .getOrCreate()

print("SparkSession berhasil diinisialisasi untuk kueri data.")
print(f"Versi Spark: {spark.version}")
print("-" * 50)

# --- Path Konfigurasi ---
# Mendapatkan direktori dasar (project_root) dari lokasi script ini
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_PARQUET_PATH = os.path.join(BASE_DIR, "data", "processed_parquet")

# --- 2. Read Data dari Parquet Format ---
# Membaca data yang sudah di-ETL dan disimpan dalam format Parquet
print(f"Membaca data dari format Parquet di: {PROCESSED_PARQUET_PATH}")
try:
    df_device_activity = spark.read.parquet(PROCESSED_PARQUET_PATH)
    print("Schema data yang dibaca dari Parquet:")
    df_device_activity.printSchema()
    print("Data yang dibaca dari Parquet (beberapa baris pertama):")
    df_device_activity.show(10, truncate=False) # truncate=False agar data tidak terpotong di konsol

    # --- 3. Mendaftarkan DataFrame sebagai Temporary View ---
    # Ini memungkinkan kita untuk menjalankan kueri SQL pada data.
    df_device_activity.createOrReplaceTempView("device_activity_logs")
    print("\nDataFrame 'df_device_activity' telah didaftarkan sebagai temporary view 'device_activity_logs'.")
    print("-" * 50)

    # --- 4. Query Data Menggunakan Apache Spark SQL ---
    print("Studi Kasus: Melihat ringkasan log aktivitas per perangkat (online, offline, standby)")

    # Kueri SQL pertama: Menghitung jumlah event berdasarkan status per device_id
    # Ini memberikan ringkasan berapa kali setiap perangkat online, offline, atau standby.
    sql_query_summary = """
        SELECT
            device_id,
            device_model,
            location,
            SUM(CASE WHEN status = 'online' THEN 1 ELSE 0 END) AS online_count,
            SUM(CASE WHEN status = 'offline' THEN 1 ELSE 0 END) AS offline_count,
            SUM(CASE WHEN status = 'standby' THEN 1 ELSE 0 END) AS standby_count,
            COUNT(status) AS total_activity_count -- Menghitung total aktivitas untuk verifikasi
        FROM
            device_activity_logs
        GROUP BY
            device_id, device_model, location
        ORDER BY
            device_id
    """
    print("\nHasil Kueri SQL: Ringkasan Aktivitas Perangkat")
    spark.sql(sql_query_summary).show(truncate=False)

    # Kueri SQL kedua: Melihat detail log aktivitas hanya untuk perangkat yang sedang "online"
    # Ini menunjukkan bagaimana Anda bisa memfilter dan menampilkan data detail.
    sql_query_online_detail = """
        SELECT
            device_id,
            device_model,
            location,
            timestamp,
            status
        FROM
            device_activity_logs
        WHERE
            status = 'online'
        ORDER BY
            device_id, timestamp
    """
    print("\nHasil Kueri SQL: Detail Log Perangkat yang ONLINE")
    spark.sql(sql_query_online_detail).show(truncate=False)

except Exception as e:
    # Penanganan kesalahan jika file Parquet belum ada atau rusak
    print(f"ERROR: Gagal membaca data Parquet. Pastikan '{PROCESSED_PARQUET_PATH}' ada dan berisi data yang benar.")
    print("Biasanya ini terjadi jika 'etl_process.py' belum dijalankan atau gagal.")
    print(f"Detail Error: {e}")

print("-" * 50)

# --- Hentikan SparkSession ---
# Penting untuk selalu menghentikan SparkSession setelah selesai agar sumber daya dilepaskan.
spark.stop()
print("SparkSession telah dihentikan.")