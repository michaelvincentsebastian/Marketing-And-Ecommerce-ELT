import pyarrow as pa
import pyarrow.flight as fl
import duckdb
import os
import sys
import time # <-- IMPORT TIME

# --- Konfigurasi ---
HOST = "localhost"
PORT = 50051
# PATH DB: localdatabase.db
LAKEHOUSE_FILE = r"B:\GitHub Repository\Marketing-And-Ecommerce-ELT\localdatabase.db" 

class DuckDBFlightServer(fl.FlightServerBase):
    def __init__(self, host, port, db_path):
        location = f"grpc://{host}:{port}"
        super().__init__(location) 
        self.db_path = db_path
        print(f"Server Flight DuckDB berjalan di: {location}")

    def do_get(self, context, ticket):
        # Mulai timer total do_get
        start_time_total = time.time()
        
        try:
            query = ticket.ticket.decode()
            print(f"\n[SERVER] Menerima kueri asli: {query}")
            
            query_processed = query 
            
            # KONEKSI LANGSUNG KE FILE DB
            with duckdb.connect(self.db_path, read_only=True) as con: 
                
                # --- MULAI TIMER EKSEKUSI DB ---
                start_time_exec = time.time()
                
                # 1. Ambil RecordBatchReader
                reader = con.execute(query_processed).fetch_record_batch() 
                
                # 2. Konversi RecordBatchReader menjadi PyArrow Table
                # (Waktu proses/baca data)
                arrow_table = reader.read_all()
                
                # --- AKHIR TIMER EKSEKUSI DB ---
                end_time_exec = time.time()
                
                # 3. Hasil kembalian ke Klien tetap berupa RecordBatchStream dari Table
                return_stream = pa.flight.RecordBatchStream(arrow_table)

            # AKHIR TIMER TOTAL DO_GET
            end_time_total = time.time()
            
            # OUTPUT WAKTU PROSES DI SERVER
            exec_duration = end_time_exec - start_time_exec
            total_duration = end_time_total - start_time_total

            print(f"[SERVER] Data siap. Jumlah Baris: {arrow_table.num_rows}.")
            print(f"[SERVER] Waktu Eksekusi DuckDB + Baca Data (Local): {exec_duration:.4f} detik")
            print(f"[SERVER] Waktu Total do_get (Termasuk Persiapan Flight): {total_duration:.4f} detik")
            
            return return_stream

        except Exception as e:
            print(f"[SERVER] Error saat memproses do_get: {e}", file=sys.stderr)
            raise pa.ArrowIOError(f"Error saat eksekusi kueri: {e}")

# --- Jalankan Server ---
def serve_duckdb():
    server = DuckDBFlightServer(HOST, PORT, LAKEHOUSE_FILE)
    
    if not os.path.exists(LAKEHOUSE_FILE):
        print(f"❌ ERROR: File DuckDB '{LAKEHOUSE_FILE}' tidak ditemukan.")
        return

    print("Tekan Ctrl+C untuk menghentikan server...")
    server.serve()

if __name__ == "__main__":
    serve_duckdb()