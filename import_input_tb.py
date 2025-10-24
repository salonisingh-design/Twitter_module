import pandas as pd
import psycopg2
from psycopg2 import sql

# --- Database connection details ---
DB_HOST = "forage-dev-db.cod4levdfbtz.ap-south-1.rds.amazonaws.com"
DB_NAME = "Kapow"
DB_USER = "kapow"
DB_PASS = "kapow123"
DB_PORT = "5432"  # default PostgreSQL port

# --- CSV file path ---
CSV_FILE = "_TwitterProductionProfileTweets__202510130929.csv"

# --- Table name in PostgreSQL ---
SCHEMA_NAME = "Twitter"   # 👈 custom schema
TABLE_NAME = "input_tb"


# 1. Read CSV file into a pandas DataFrame
df = pd.read_csv(CSV_FILE)

# 2. Connect to PostgreSQL
conn = psycopg2.connect(
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    port=DB_PORT
)
cur = conn.cursor()

cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA_NAME)))

# 4. Create the table inside the schema
columns = list(df.columns)
create_table_query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {}.{} (
        {}
    )
""").format(
    sql.Identifier(SCHEMA_NAME),
    sql.Identifier(TABLE_NAME),
    sql.SQL(", ").join(
        sql.SQL("{} TEXT").format(sql.Identifier(col)) for col in columns
    )
)
cur.execute(create_table_query)
conn.commit()

# 5. Insert the CSV data into the table
for _, row in df.iterrows():
    insert_query = sql.SQL("""
        INSERT INTO {}.{} ({})
        VALUES ({})
    """).format(
        sql.Identifier(SCHEMA_NAME),
        sql.Identifier(TABLE_NAME),
        sql.SQL(', ').join(map(sql.Identifier, columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(columns))
    )
    cur.execute(insert_query, tuple(row))
conn.commit()

# 6. Close the connection
cur.close()
conn.close()

print(f"✅ Data imported successfully into {SCHEMA_NAME}.{TABLE_NAME}.")