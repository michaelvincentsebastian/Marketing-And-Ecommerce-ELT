import duckdb

con = duckdb.connect()
con.sql("INSTALL httpfs; LOAD httpfs;")
con.sql("INSTALL ducklake; LOAD ducklake;")

minioAuth = {
    'accessKey': 'minioadmin',
    'secretKey': 'miniopassword',
    'bucket': 'marketing-and-ecommerce',
    'endpoint': 'localhost:9000'
}

# --- 1. Konfigurasi MinIO (S3 API) ---
con.sql(f"SET s3_endpoint = '{minioAuth['endpoint']}';") 
con.sql(f"SET s3_access_key_id = '{minioAuth['accessKey']}';")
con.sql(f"SET s3_secret_access_key = '{minioAuth['secretKey']}';")
con.sql("SET s3_use_ssl = false;") # Wajib untuk http (tanpa ssl)
con.sql("SET s3_url_style = 'path';") # Wajib untuk MinIO

# --- 2. Setup Path & Catalog ---
catalogMetadataPath = './catalog.ducklake' # Path Metadata Catalog
managedStoredDataPath = f"s3://{minioAuth['bucket']}/managedStoreData/" 
databasePath = '../localstagingdatabase.db' # Path database SQLMesh lokal

# --- Daftar Tabel (Mapping Nama Pendek ke Nama Lengkap) ---
seedTables = {
    'campaigns': 'datamarketingseeds.campaigns',
    'customers': 'datamarketingseeds.customers',
    'events': 'datamarketingseeds.events',
    'products': 'datamarketingseeds.products',
    'transactions': 'datamarketingseeds.transactions'
}

cleanTables = {
    # Perhatikan skema yang Anda tentukan di model clean: datamarketingseeds.clean_...
    'clean_campaigns': 'datamarketingseeds.clean_campaigns',
    'clean_customers': 'datamarketingseeds.clean_customers',
    'clean_events': 'datamarketingseeds.clean_events',
    'clean_products': 'datamarketingseeds.clean_products',
    'clean_transactions': 'datamarketingseeds.clean_transactions'
}

modelTables = {
    'customer_rfm': 'datamarketingseeds.dim_customer_rfm', 
    'net_revenue': 'datamarketingseeds.fct_transactions_net_revenue'
}

# --- 3. Attach Database & Catalog ---

# Attach duckdb ke sumber database (databasePath)
con.sql(f"ATTACH '{databasePath}' AS source_db;")
print(f"✅ Berhasil attach database sumber: {databasePath}\n")

# Attach DuckLake Catalog (yang akan menulis Parquet ke MinIO)
# Nama Katalog: datalakehouse
con.sql(f"ATTACH 'ducklake:{catalogMetadataPath}' AS datalakehouse (DATA_PATH '{managedStoredDataPath}');")
print(f"✅ Berhasil attach DuckLake catalog 'datalakehouse'. Data akan ditulis ke: {managedStoredDataPath}\n")

# --- 4. Fungsi Pembantu untuk Migrasi ---

def migrate_table(source_full_name: str, target_short_name: str):
    """Melakukan CREATE TABLE AS SELECT dari source_db ke datalakehouse."""
    
    # Target harus eksplisit menggunakan katalog datalakehouse
    # Contoh target: datalakehouse.datamarketingseeds.campaigns
    target_table = f"datalakehouse.{source_full_name}" 
    
    # Source harus eksplisit menggunakan alias source_db
    # Contoh source: source_db.datamarketingseeds.campaigns
    source_table = f"source_db.{source_full_name}"
    
    print(f"Migrasi {target_short_name}: {source_table} -> {target_table}...")

    try:
        # 1. Drop tabel lama di DuckLake jika sudah ada (untuk percobaan ulang)
        con.sql(f"DROP TABLE IF EXISTS {target_table};")
        
        # 2. Perintah Migrasi: CREATE TABLE AS SELECT
        migration_sql = f"""
        CREATE TABLE {target_table} AS 
        SELECT * FROM {source_table};
        """
        con.sql(migration_sql)
        print(f"   [SUKSES] {target_short_name} dimigrasikan.\n")

    except Exception as e:
        print(f"   [GAGAL]: Terjadi Error saat migrasi {target_short_name}. Error: {e} \n")
        
# --- 5. Jalankan Migrasi Manual per Kelompok ---

# Migrasi Seed / Raw Data
print("--- Memulai Migrasi SEED/RAW Tabel ---")
migrate_table(seedTables['campaigns'], 'campaigns')
migrate_table(seedTables['customers'], 'customers')
migrate_table(seedTables['events'], 'events')
migrate_table(seedTables['products'], 'products')
migrate_table(seedTables['transactions'], 'transactions')

# Migrasi Clean Data
print("--- Memulai Migrasi CLEAN Tabel ---")
migrate_table(cleanTables['clean_campaigns'], 'clean_campaigns')
migrate_table(cleanTables['clean_customers'], 'clean_customers')
migrate_table(cleanTables['clean_events'], 'clean_events')
migrate_table(cleanTables['clean_products'], 'clean_products')
migrate_table(cleanTables['clean_transactions'], 'clean_transactions')
 
# Migrasi Model Data
print("--- Memulai Migrasi MODEL DATA Tabel ---")
migrate_table(modelTables['customer_rfm'], 'customer_rfm')
migrate_table(modelTables['net_revenue'], 'net_revenue')
 
print("--- ✅ Semua Migrasi Selesai! ---")

con.close()