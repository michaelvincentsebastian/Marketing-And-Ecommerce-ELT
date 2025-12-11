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
    'bucket': 'marketing-and-ecommerce',
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
    print(f"✅ Bucket '{minioAuth['bucket']}' already exists. \n")
    
except Exception as e:
    # Jika bucket not found / 404 -> buat bucket baru
    if '404' in str(e):
            s3_client.create_bucket(Bucket = minioAuth['bucket'])
            print(f"✅ Bucket '{minioAuth['bucket']}' created successfully. \n") 
    # Jika error bukan not found / 404 (artinya ada kesalahan lain, biasanya salah endpoint api)
    else:
        print(f"❌ An error occurred while checking/creating the bucket: {e} \n")
        
# --------------------------
# Setup Target Raw File Path
# --------------------------

# Path Database Catalog untuk Mengelola Data dan Metadata
catalogMetadata = './dataLakehouse/catalog.ducklake'
# Path Folder MINIO tempat dimana file dikelola oleh database catalog ducklake
managedStoredData = f"s3://{minioAuth['bucket']}/ducklake/"
# File Path untuk Raw data yang akan dikelola
localRawDataPath = {
    'campaigns': r'C:/Users/Asus/Downloads/Data Andalan Utama (Intern)/Marketing & E-Commerce Analytics/rawData/campaigns.csv',
    'customers': r'C:/Users/Asus/Downloads/Data Andalan Utama (Intern)/Marketing & E-Commerce Analytics/rawData/customers.csv',
    'events': r'C:/Users/Asus/Downloads/Data Andalan Utama (Intern)/Marketing & E-Commerce Analytics/rawData/events.csv',
    'products': r'C:/Users/Asus/Downloads/Data Andalan Utama (Intern)/Marketing & E-Commerce Analytics/rawData/products.csv',
    'transactions': r'C:/Users/Asus/Downloads/Data Andalan Utama (Intern)/Marketing & E-Commerce Analytics/rawData/transactions.csv'
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
loadCampaign = f"""
        CREATE OR REPLACE TABLE raw.campaigns AS 
        SELECT * FROM read_csv_auto('{localRawDataPath['campaigns']}');
    """
loadCustomer = f"""
        CREATE OR REPLACE TABLE raw.customers AS 
        SELECT * FROM read_csv_auto('{localRawDataPath['customers']}');
    """
loadEvents = f"""
        CREATE OR REPLACE TABLE raw.events AS 
        SELECT * FROM read_csv_auto('{localRawDataPath['events']}');
    """
loadProduct = f"""
        CREATE OR REPLACE TABLE raw.products AS 
        SELECT * FROM read_csv_auto('{localRawDataPath['products']}');
    """
loadTransaction = f"""
        CREATE OR REPLACE TABLE raw.transactions AS 
        SELECT * FROM read_csv_auto('{localRawDataPath['transactions']}');
    """
    
# Load Datanya
try:
    con.sql(loadCustomer)
    con.sql(loadCampaign)
    con.sql(loadEvents)
    con.sql(loadProduct)
    con.sql(loadTransaction)
    print(f"✅ Semua tabel raw terdaftar.")

except Exception as e:
    print(f"❌ Gagal memuat tabel | Error: {e}")

# --------------------------------------------------
# Setelah Node Selesai Matikan Connection DuckDB nya
# --------------------------------------------------

con.close()