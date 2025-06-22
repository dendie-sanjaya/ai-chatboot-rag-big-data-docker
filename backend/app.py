import os
import re
import json
import requests
from flask import Flask, request, jsonify, Response, stream_with_context
from flask_cors import CORS
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import sqlite3
import atexit

app = Flask(__name__)
CORS(app)

# --- 1. Configure LLM API (Sekarang ke Ollama) ---
load_dotenv()
OLLAMA_API_URL = os.getenv('OLLAMA_API_URL', 'http://chatbot-ollama-service:11434/api/generate')
OLLAMA_MODEL_NAME = os.getenv('OLLAMA_MODEL_NAME', 'deepseek-coder:1.5b')

print(f"Menggunakan Ollama API di: {OLLAMA_API_URL} dengan model: {OLLAMA_MODEL_NAME}")

# --- 2. Initialize Spark Session and Load Device Data ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_PARQUET_PATH = os.path.join(BASE_DIR, "data", "processed_parquet")

spark = None
df_device_activity = None

def init_spark_and_load_data():
    global spark, df_device_activity

    spark = SparkSession.builder \
        .appName("ChatbotSparkIntegration") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    print("Apache Spark Session initialized.")
    print(f"Spark Version: {spark.version}")

    print(f"Attempting to read device activity data from Parquet at: {PROCESSED_PARQUET_PATH}")
    try:
        df_device_activity = spark.read.parquet(PROCESSED_PARQUET_PATH)
        df_device_activity.createOrReplaceTempView("device_activity_logs")
        print("Device Activity DataFrame loaded from Parquet.")
        print("Schema from Parquet:")
        df_device_activity.show(5, truncate=False)
    except Exception as e:
        df_device_activity = None
        print(f"WARNING: Could not load device activity data from Parquet. "
              f"Ensure '{PROCESSED_PARQUET_PATH}' exists and contains valid Parquet files. "
              f"Detail: {e}")
        print("Device activity related queries might not work.")

# --- 3. Function to Retrieve Data from Spark DataFrames ---
def get_data_from_spark(table_name, condition_col=None, condition_val=None, like_match=False, limit=None, order_by_col=None, order_desc=True):
    global spark, df_device_activity

    df = None
    if table_name == "device_activity_logs":
        df = df_device_activity
        if df is None:
            print(f"Error: DataFrame for '{table_name}' is not loaded.")
            return []
    else:
        print(f"Error: Unknown table name '{table_name}' or DataFrame not loaded.")
        return []

    if df is None:
        print(f"Error: DataFrame for '{table_name}' is not initialized.")
        return []

    if condition_col and condition_val:
        if like_match:
            df = df.filter(col(condition_col).like(f"%{condition_val}%"))
        else:
            df = df.filter(col(condition_col) == condition_val)

    if order_by_col and order_by_col in df.columns:
        if order_desc:
            df = df.orderBy(desc(order_by_col))
        else:
            df = df.orderBy(col(order_by_col))

    if limit:
        df = df.limit(limit)

    try:
        return df.collect()
    except Exception as e:
        print(f"Error collecting data from Spark DataFrame: {e}")
        return []

# --- 4. Intent Detection and Entity Extraction ---
def detect_intent_and_extract_entities(user_query):
    query_lower = user_query.lower()
    intent = "unknown"
    entities = {}

    if "status" in query_lower or "cek" in query_lower or "perangkat" in query_lower or "device" in query_lower:
        intent = "check_device_status"

        match_device_id = re.search(r'([a-z]{2,5}\d{2,5}|[a-z]{3,5}-\w{3}-\d{3})', query_lower)
        if match_device_id:
            entities['device_id'] = match_device_id.group(1).upper()

        match_device_model = re.search(r'(olt|router|server|switch|huawei|cisco|zte|samsung|dev \d+|model \w+)', query_lower)
        if match_device_model:
            entities['device_model_query'] = match_device_model.group(0)

    return intent, entities

# --- 5. Function to Retrieve Context from Spark Data ---
def get_context_from_spark_data(intent, entities):
    context = ""
    if intent == "check_device_status":
        device_id_specific = entities.get('device_id')
        device_model_query = entities.get('device_model_query')

        if device_id_specific:
            print(f"DEBUG: Mencari perangkat dengan ID spesifik: {device_id_specific}")
            result = get_data_from_spark("device_activity_logs", "device_id", device_id_specific,
                                         like_match=False, limit=1, order_by_col="timestamp", order_desc=True)
            if result:
                row = result[0]
                context = (
                    f"Konteks: Status perangkat {row.device_id} ({row.device_model if hasattr(row, 'device_model') else 'Model Tidak Diketahui'}): "
                    f"Saat ini {row.status} di {row.location if hasattr(row, 'location') else 'Lokasi Tidak Diketahui'}. "
                    f"Terakhir tercatat pada {row.timestamp if hasattr(row, 'timestamp') else 'Waktu Tidak Diketahui'}."
                )
            else:
                if device_model_query:
                    print(f"DEBUG: ID spesifik '{device_id_specific}' tidak ditemukan. Mencoba mencari berdasarkan model '{device_model_query}'.")
                    result = get_data_from_spark("device_activity_logs", "device_model", device_model_query,
                                                  like_match=True, limit=5, order_by_col="timestamp", order_desc=True)
                    if result:
                        context_parts = [f"Konteks: Perangkat dengan ID '{device_id_specific}' tidak ditemukan secara eksak, namun berikut beberapa perangkat dengan model serupa ({device_model_query}) atau yang terkait:"]
                        seen_devices = set()
                        for row in result:
                            if row.device_id not in seen_devices:
                                context_parts.append(f"- {row.device_id} ({row.device_model if hasattr(row, 'device_model') else 'Model Tidak Diketahui'}): {row.status} di {row.location if hasattr(row, 'location') else 'Lokasi Tidak Diketahui'}. Terakhir tercatat pada {row.timestamp if hasattr(row, 'timestamp') else 'Waktu Tidak Diketahui'}."
                                )
                                seen_devices.add(row.device_id)
                        context = "\n".join(context_parts)
                    else:
                        context = f"Konteks: Maaf, data status untuk perangkat '{device_id_specific}' atau model '{device_model_query}' tidak ditemukan di log aktivitas."
                else:
                    context = f"Konteks: Maaf, data status untuk perangkat '{device_id_specific}' tidak ditemukan di log aktivitas."

        elif device_model_query:
            print(f"DEBUG: Mencari perangkat dengan model umum: {device_model_query}")
            result = get_data_from_spark("device_activity_logs", "device_model", device_model_query,
                                          like_match=True, limit=5, order_by_col="timestamp", order_desc=True)
            if result:
                context_parts = [f"Konteks: Berikut adalah beberapa status terbaru untuk perangkat dengan model '{device_model_query}':"]
                seen_devices = set()
                for row in result:
                    if row.device_id not in seen_devices:
                        context_parts.append(f"- {row.device_id} ({row.device_model if hasattr(row, 'device_model') else 'Model Tidak Diketahui'}): {row.status} di {row.location if hasattr(row, 'location') else 'Lokasi Tidak Diketahui'}. Terakhir tercatat pada {row.timestamp if hasattr(row, 'timestamp') else 'Waktu Tidak Diketahui'}."
                        )
                        seen_devices.add(row.device_id)
                context = "\n".join(context_parts)
            else:
                context = f"Konteks: Tidak ditemukan informasi log aktivitas untuk perangkat dengan model '{device_model_query}'."
        else:
            context = "Konteks: Pengguna ingin mengetahui status perangkat secara umum. Informasikan bahwa Anda memerlukan ID perangkat yang spesifik atau model perangkat (misalnya, OLT Huawei, Router Cisco) untuk detail lebih lanjut, atau tanyakan ID perangkat apa yang ingin diketahui statusnya."
    else:
        context = "Konteks: Pertanyaan ini tidak terkait dengan data perangkat yang saya miliki. Saya hanya bisa memberikan informasi mengenai status perangkat."
    return context

# --- 6. Main Function to Interact with Ollama (Streaming) ---
def generate_ollama_response_stream(user_query):
    intent, entities = detect_intent_and_extract_entities(user_query)
    db_context = get_context_from_spark_data(intent, entities)

    llm_role = "Anda adalah customer service yang ramah dan responsif, khusus untuk layanan jaringan dan pemantauan perangkat."

    prompt_parts = [
        f"{llm_role}",
        db_context,
        "---",
        f"Pertanyaan Pengguna: {user_query}",
        "---",
        "Berikan jawaban yang ramah, informatif, dan ringkas berdasarkan konteks yang diberikan. ",
        "Jika konteks dari data perangkat tidak memberikan informasi yang cukup, katakan bahwa Anda tidak memiliki data spesifik atau butuh informasi lebih lanjut. ",
        "Jika pertanyaan tidak terkait dengan perangkat, informasikan bahwa Anda hanya bisa memberikan informasi status perangkat.",
        "**Penting: Pastikan respons Anda hanya berisi jawaban akhir, tanpa proses berpikir internal, catatan debugging, atau tag seperti <think>.**",
        "---",
        "Jawaban:"
    ]

    full_prompt = "\n".join(filter(None, prompt_parts))

    ollama_payload = {
        "model": OLLAMA_MODEL_NAME,
        "prompt": full_prompt,
        "stream": True
    }

    try:
        response = requests.post(OLLAMA_API_URL, json=ollama_payload, stream=True)
        response.raise_for_status()

        # Inisialisasi flag dan buffer untuk mengontrol streaming
        will_stream_to_frontend = False
        current_buffer = ""

        # Regex untuk menemukan tag penutup </think>
        end_think_tag_pattern = re.compile(r'</think>', re.DOTALL)

        for line in response.iter_lines():
            if line:
                try:
                    chunk = json.loads(line.decode('utf-8'))
                    if 'response' in chunk:
                        ollama_text = chunk['response']
                        
                        # Selalu tambahkan teks chunk ke buffer
                        current_buffer += ollama_text

                        # Logika filtering berdasarkan flag
                        if not will_stream_to_frontend:
                            # Cari tag penutup </think> di dalam buffer
                            match_end_think = end_think_tag_pattern.search(current_buffer)

                            if match_end_think:
                                # Jika </think> ditemukan, kita sudah bisa mulai streaming
                                # Ambil teks yang ada setelah </think>
                                # Kita TIDAK menggunakan .strip() di sini untuk mempertahankan spasi awal jika ada
                                text_after_think = current_buffer[match_end_think.end():]
                                will_stream_to_frontend = True # Set flag menjadi True
                                
                                # Reset buffer karena bagian <think> sudah diproses
                                current_buffer = "" 

                                # Kirimkan teks yang tersisa (yang seharusnya adalah respons sebenarnya)
                                # Pastikan tidak mengirim string kosong
                                if text_after_think: # Cukup cek apakah ada teks, bukan spasi murni
                                    yield json.dumps({"text": text_after_think}) + "\n"
                            else:
                                # Jika </think> belum ditemukan, biarkan teks menumpuk di buffer
                                # dan jangan kirimkan apa-apa ke frontend (karena masih dalam blok <think>)
                                pass 
                        else:
                            # Jika will_stream_to_frontend sudah True, berarti kita sudah melewati <think>
                            # Kirimkan teks dari buffer saat ini (yang sudah bersih dari <think>)
                            # dan reset buffer untuk chunk berikutnya.
                            # Kita TIDAK menggunakan .strip() di sini
                            if current_buffer: # Cukup cek apakah ada teks, bukan spasi murni
                                yield json.dumps({"text": current_buffer}) + "\n"
                            current_buffer = "" # Reset buffer setelah dikirim

                    elif 'error' in chunk:
                        print(f"Ollama API error in chunk: {chunk['error']}")
                        yield json.dumps({"error": f"Ollama API Error: {chunk['error']}"}) + "\n"
                except json.JSONDecodeError:
                    print(f"Could not decode JSON from Ollama stream: {line.decode('utf-8')}")
                    continue

    except requests.exceptions.RequestException as e:
        print(f"Error connecting to Ollama API: {e}")
        yield json.dumps({"error": f"Maaf, tidak dapat terhubung ke model AI. Pastikan Ollama berjalan dan model '{OLLAMA_MODEL_NAME}' tersedia. Detail: {str(e)}"}) + "\n"
    except Exception as e:
        print(f"Unexpected error during Ollama interaction: {e}")
        yield json.dumps({"error": f"Maaf, ada masalah teknis di server: {str(e)}"}) + "\n"


# --- Flask API Endpoint ---
@app.route('/chat', methods=['POST'])
def chat_endpoint():
    user_message = request.json.get('message')

    if not user_message:
        return jsonify({"error": "Pesan tidak boleh kosong"}), 400

    return Response(stream_with_context(generate_ollama_response_stream(user_message)), mimetype='application/x-ndjson')

# --- Inisialisasi Spark dan Load Data saat aplikasi Flask dimulai ---
with app.app_context():
    init_spark_and_load_data()

if __name__ == '__main__':
    def stop_spark_on_exit():
        global spark
        if spark:
            spark.stop()
            print("SparkSession stopped gracefully.")
    atexit.register(stop_spark_on_exit)

    app.run(host='0.0.0.0', port=5000, debug=True)