import pyarrow as pa
import pyarrow.flight as fl
import duckdb
import os
import sys

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
        try:
            query = ticket.ticket.decode()
            print(f"\n[SERVER] Menerima kueri asli: {query}")
            
            query_processed = query 
            
            # KONEKSI LANGSUNG KE FILE DB
            with duckdb.connect(self.db_path, read_only=True) as con: 
                
                # JANGAN GUNAKAN .arrow() di sini. Kita akan ambil RecordBatchReader,
                # lalu baca semua datanya menjadi PyArrow Table (Table memiliki num_rows).
                
                # 1. Ambil RecordBatchReader
                reader = con.execute(query_processed).fetch_record_batch() 
                
                # 2. Konversi RecordBatchReader menjadi PyArrow Table (Table punya num_rows)
                arrow_table = reader.read_all() # <-- SOLUSI ATAS AttributeError
                
                # 3. Hasil kembalian ke Klien tetap berupa RecordBatchStream dari Table
                return_stream = pa.flight.RecordBatchStream(arrow_table)

            print(f"[SERVER] Mengirimkan {arrow_table.num_rows} baris.")
            return return_stream # Mengembalikan stream yang dibuat dari Table

        except Exception as e:
            # JIKA ERROR Catalog "main" does not exist! MUNCUL LAGI, 
            # kita harus kembali ke ATTACH AS localdatabase.
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