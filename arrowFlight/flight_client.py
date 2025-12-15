import pyarrow.flight as fl
import pandas as pd
import time
import os
import pyarrow.parquet as pq

# --- Konfigurasi ---
HOST = "localhost"
PORT = 50051
OUTPUT_DIR = "./arrowFlight/data/"
OUTPUT_FILENAME = "cleanEvents.parquet"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)

def run_client():
    # 1. Pastikan folder output ada
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"[CLIENT] Membuat direktori output: ./{OUTPUT_DIR}")

    client = fl.FlightClient(f"grpc://{HOST}:{PORT}")
    print(f"[CLIENT] Terhubung ke Server Flight di {HOST}:{PORT}")
    
    # Kueri SQL yang dikirim (Hanya Skema dan Tabel, tanpa Katalog)
    query = "SELECT * FROM datamarketingclean.events;" 
    
    # 2. Buat Ticket dari kueri SQL
    ticket = fl.Ticket(query.encode())
    
    start_time = time.time()
    try:
        # 3. Lakukan permintaan data (do_get)
        reader = client.do_get(ticket)
        
        # 4. Baca seluruh aliran data ke PyArrow Table
        arrow_table = reader.read_all()
        
        # 5. Konversi ke Pandas DataFrame (untuk overview data)
        df = arrow_table.to_pandas()
        
        end_time = time.time()
        
        print("\n" + "="*50)
        print("✅ Transfer Data Selesai!")
        print(f"   Jumlah Baris Diterima: {len(df)}")
        print(f"   Waktu Transfer (Flight): {end_time - start_time:.4f} detik")
        print("="*50)
        
        # --- 6. Penyimpanan Data ke Parquet ---
        print(f"📦 Menyimpan data ke file Parquet di: {OUTPUT_PATH}...")
        pq.write_table(arrow_table, OUTPUT_PATH)
        print("✅ Penyimpanan file Parquet berhasil.")
        
        # --- 7. Overview Data ---
        print("\n5 Baris Pertama Data (DataFrame Pandas):")
        print(df.head())
        
    except Exception as e:
        print(f"❌ [CLIENT] Gagal mengambil atau menyimpan data: {e}")

if __name__ == "__main__":
    run_client()