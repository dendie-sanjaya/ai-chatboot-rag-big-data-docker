# scripts/etl_process.py (MODIFIKASI)

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import functions as F
import os

# --- Path Konfigurasi ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw")
OUTPUT_PARQUET_PATH = os.path.join(BASE_DIR, "processed_parquet")
SQLITE_DB_PATH = os.path.join(RAW_DATA_PATH, "device_info.db")

# --- 1. Inisialisasi SparkSession ---
# Untuk membaca dari JDBC (SQLite), kita perlu menambahkan JDBC driver ke classpath Spark.
# Gunakan .config("spark.jars", "path/to/sqlite-jdbc-X.X.X.jar")
# Ganti "path/to/sqlite-jdbc-X.X.X.jar" dengan jalur sebenarnya ke driver JAR Anda.
# Contoh: Jika Anda meletakkannya di project_root/lib/
SQLITE_JDBC_DRIVER_PATH = os.path.join(BASE_DIR, "ai-spark","lib", "sqlite-jdbc-3.49.1.0.jar") # SESUAIKAN VERSI DRIVER DAN PATH NYA!

spark = SparkSession.builder \
    .appName("DeviceActivityETL") \
    .master("local[*]") \
    .config("spark.jars", SQLITE_JDBC_DRIVER_PATH) \
    .getOrCreate()

print("SparkSession berhasil diinisialisasi untuk proses ETL.")
print(f"Versi Spark: {spark.version}")
print("-" * 50)

# Pastikan direktori output ada dan bersih
if os.path.exists(OUTPUT_PARQUET_PATH):
    import shutil
    shutil.rmtree(OUTPUT_PARQUET_PATH)
    print(f"Direktori '{OUTPUT_PARQUET_PATH}' telah dibersihkan.")

# --- 2. Get Data dari Berbagai Sumber ---

# Datasource 1: Data Info Perangkat (DARI SQLITE DATABASE)
print(f"Membaca data info perangkat dari SQLite database: {SQLITE_DB_PATH}...")
try:
    jdbc_url = f"jdbc:sqlite:{SQLITE_DB_PATH}"
    db_properties = {
        "driver": "org.sqlite.JDBC" # Nama kelas driver SQLite JDBC
    }

    df_device_info = spark.read \
        .jdbc(url=jdbc_url, table="device_info", properties=db_properties)

    print("Schema df_device_info (dari SQLite):")
    df_device_info.printSchema()
    print("Data df_device_info (dari SQLite):")
    df_device_info.show()

except Exception as e:
    print(f"ERROR: Gagal membaca dari SQLite. Pastikan database ada dan driver JDBC benar. Error: {e}")
    # Hentikan Spark jika gagal membaca dari DB, karena ini data penting
    spark.stop()
    exit(1) # Keluar dari script

# Datasource 2: Data Log Aktivitas (Dari File Text - CSV)
print("Membaca data log aktivitas dari CSV...")
activity_schema = StructType([
    StructField("log_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("status", StringType(), True)
])

df_activity_logs = spark.read \
    .option("header", "true") \
    .schema(activity_schema) \
    .csv(os.path.join(RAW_DATA_PATH, "device_activity_logs.csv"))

print("Schema df_activity_logs:")
df_activity_logs.printSchema()
print("Data df_activity_logs:")
df_activity_logs.show()

print("-" * 50)

# --- 3. Transformasi Data (Contoh Join dan Seleksi Kolom) ---
print("Melakukan join data info perangkat dan log aktivitas berdasarkan 'device_id'...")
df_joined_data = df_activity_logs.join(
    df_device_info,
    on="device_id",
    how="inner"
)

df_transformed = df_joined_data.select(
    "device_id",
    "device_model",
    "location",
    "timestamp",
    "status"
).orderBy("timestamp")

print("Schema data setelah transformasi:")
df_transformed.printSchema()
print("Data setelah transformasi (beberapa baris pertama):")
df_transformed.show()

print("-" * 50)

# --- 4. Write Result ETL ke Parquet Format ---
print(f"Menulis hasil transformasi ke format Parquet di: {OUTPUT_PARQUET_PATH}")
df_transformed.write \
    .mode("overwrite") \
    .parquet(OUTPUT_PARQUET_PATH)

print("Proses ETL selesai. Data berhasil disimpan dalam format Parquet.")
print("-" * 50)

# --- Hentikan SparkSession ---
spark.stop()
print("SparkSession telah dihentikan.")