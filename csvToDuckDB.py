import duckdb
import time

startTime = time.time()

csvFilePath = {
    'campaigns': r'C:/Users/Asus/Downloads/Data Andalan Utama (Intern)/Marketing & E-Commerce Analytics/rawData/campaigns.csv',
    'customers': r'C:/Users/Asus/Downloads/Data Andalan Utama (Intern)/Marketing & E-Commerce Analytics/rawData/customers.csv',
    'events': r'C:/Users/Asus/Downloads/Data Andalan Utama (Intern)/Marketing & E-Commerce Analytics/rawData/events.csv',
    'products': r'C:/Users/Asus/Downloads/Data Andalan Utama (Intern)/Marketing & E-Commerce Analytics/rawData/products.csv',
    'transactions': r'C:/Users/Asus/Downloads/Data Andalan Utama (Intern)/Marketing & E-Commerce Analytics/rawData/transactions.csv'
}

con = duckdb.connect('./datastore.duckdb')

con.sql("CREATE SCHEMA IF NOT EXISTS raw")

try:
    con.sql(F"CREATE OR REPLACE TABLE raw.campaigns AS SELECT * FROM read_csv_auto('{csvFilePath['campaigns']}')")
    con.sql(F"CREATE OR REPLACE TABLE raw.customers AS SELECT * FROM read_csv_auto('{csvFilePath['customers']}')")
    con.sql(F"CREATE OR REPLACE TABLE raw.events AS SELECT * FROM read_csv_auto('{csvFilePath['events']}')")
    con.sql(F"CREATE OR REPLACE TABLE raw.products AS SELECT * FROM read_csv_auto('{csvFilePath['products']}')")
    con.sql(F"CREATE OR REPLACE TABLE raw.transactions AS SELECT * FROM read_csv_auto('{csvFilePath['transactions']}')")
except Exception as e:
    print(e)

endTime = time.time()

totalTime = endTime - startTime

print(f"\n✅ Done. Total Execution Time: {totalTime:.4f} seconds\n")

con.close()