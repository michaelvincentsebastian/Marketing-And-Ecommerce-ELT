# -------------
# Library Setup
# -------------

import duckdb
import boto3

# --------------------------------------------------------
# Conect ke DuckDB -> Install Extension: DuckLake & httpfs
# --------------------------------------------------------

con = duckdb.connect()
con.sql("INSTALL httpfs; LOAD httpfs;")
con.sql("INSTALL ducklake; LOAD ducklake;")

# -----------
# Minio Setup 
# -----------

# Minio Access
minioAuth = {
    'accessKey': 'minioadmin',
    'secretKey': 'miniopassword',
    'bucket': 'elt-dapodik',
    'endpoint': 'http://localhost:9000'
}

# For DuckDB (Ducklake)
con.sql(f"SET s3_endpoint = '{minioAuth['endpoint'].replace('http://', '')}';")
con.sql(f"SET s3_access_key_id = '{minioAuth['accessKey']}';")
con.sql(f"SET s3_secret_access_key = '{minioAuth['secretKey']}';")
con.sql("SET s3_use_ssl = false;") # Wajib untuk http (tanpa ssl)
con.sql("SET s3_url_style = 'path';") # Wajib untuk MinIO

# For Boto3 (Create Bucket)
s3_client = boto3.client(
    's3',
    endpoint_url = minioAuth['endpoint'],
    aws_access_key_id = minioAuth['accessKey'],
    aws_secret_access_key= minioAuth['secretKey'],
    verify = False
)

# -------------------------------
# Create Bucket if doesn't exists
# -------------------------------

try:
    # Jika sudah ada
    s3_client.head_bucket(Bucket = minioAuth['bucket'])
    print(f"Bucket '{minioAuth['bucket']}' already exists.")
    
except Exception as e:
    # Jika bucket not found / 404 -> buat bucket baru
    if '404' in str(e):
            s3_client.create_bucket(Bucket = minioAuth['bucket'])
            print(f"Bucket '{minioAuth['bucket']}' created successfully.") 
    # Jika error bukan not found / 404 (artinya ada kesalahan lain, biasanya salah endpoint api)
    else:
        print(f"An error occurred while checking/creating the bucket: {e}")
        
# --------------------------
# Setup Target Raw File Path
# --------------------------

# Path Database Catalog untuk Mengelola Data dan Metadata
catalogMetadata = './dataLakehouse/catalog.ducklake'
# Path Folder MINIO tempat dimana file dikelola oleh database catalog ducklake
managedStoredData = f"s3://{minioAuth['bucket']}/ducklake/"
# File Path untuk Raw data yang akan dikelola
localRawDataPath = {
    'kemendikdasmen': r'C:/Users/Asus/Downloads/Data Andalan Utama (Intern)/Kemendikdasmen_Sekolah di Indonesia.csv',
    'jakarta': r'C:/Users/Asus/Downloads/Data Andalan Utama (Intern)/Exercise_Data_Sekolah/rawData/Dapodik_DKI Jakarta.csv',
    'bekasi': r'B:/GitHub Repository/Scraping-Dapodik-Data/result/data_Bekasi.csv',
    'depok': r'B:/GitHub Repository/Scraping-Dapodik-Data/result/data_Depok.csv',
    'balikpapan': r'B:/GitHub Repository/Scraping-Dapodik-Data/result/data_Balikpapan.csv',
    'makassar': r'B:/GitHub Repository/Scraping-Dapodik-Data/result/data_Makassar.csv',
    'palembang': r'B:/GitHub Repository/Scraping-Dapodik-Data/result/data_Palembang.csv'
}

# ---------------
# Setup DuckLake
# ---------------

# ATTACH Database Catalog Ducklake dan tentukan dimana lokasi data akan dikelola oleh ducklake (data file parquet)
con.sql(f"ATTACH 'ducklake:{catalogMetadata}' AS dataLakehouse (DATA_PATH '{managedStoredData}');")
con.sql("USE dataLakehouse;")

# Buat Skema Jika belum Pernah dibuat
con.sql("CREATE SCHEMA IF NOT EXISTS raw;")

# Variable Load Data
loadKemendikdasmen = f"""
        CREATE OR REPLACE TABLE raw.dataKemendikdasmen AS 
        SELECT * FROM read_csv_auto('{localRawDataPath['kemendikdasmen']}');
    """
loadJakarta = f"""
        CREATE OR REPLACE TABLE raw.dataJakarta AS 
        SELECT * FROM read_csv_auto('{localRawDataPath['jakarta']}');
    """
loadBekasi = f"""
        CREATE OR REPLACE TABLE raw.dataBekasi AS 
        SELECT * FROM read_csv_auto('{localRawDataPath['bekasi']}');
    """
loadDepok = f"""
        CREATE OR REPLACE TABLE raw.dataDepok AS 
        SELECT * FROM read_csv_auto('{localRawDataPath['depok']}');
    """
loadBalikpapan = f"""
        CREATE OR REPLACE TABLE raw.dataBalikpapan AS 
        SELECT * FROM read_csv_auto('{localRawDataPath['balikpapan']}');
    """
loadMakassar = f"""
        CREATE OR REPLACE TABLE raw.dataMakassar AS 
        SELECT * FROM read_csv_auto('{localRawDataPath['makassar']}');
    """
loadPalembang = f"""
        CREATE OR REPLACE TABLE raw.dataPalembang AS 
        SELECT * FROM read_csv_auto('{localRawDataPath['palembang']}');
    """
    
# Load Datanya
try:
    con.sql(loadKemendikdasmen)
    con.sql(loadJakarta)
    con.sql(loadBekasi)
    con.sql(loadDepok)
    con.sql(loadBalikpapan)
    con.sql(loadMakassar)
    con.sql(loadPalembang)
    print(f"✅ Semua tabel raw terdaftar.")

except Exception as e:
    print(f"❌ Gagal memuat tabel | Error: {e}")

# --------------------------------------------------
# Setelah Node Selesai Matikan Connection DuckDB nya
# --------------------------------------------------

con.close()