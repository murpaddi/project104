import os
import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://postgres:GX1YNKqnlbCVPzTi@db.uplrxiyofnwfxnqkhwkm.supabase.co:5432/postgres?sslmode=require"

#Connect to database
print("Connecting to db...")
engine = create_engine(DB_URL, pool_pre_ping=True)

#Query database to test connection
query = text ("""
    SELECT *
    FROM smartbins.archive_bin_data
    WHERE sensor_id = 'R718X-001'
    ORDER BY timestamp DESC;
""")

print("Running query...")
df = pd.read_sql_query(query, engine)

if df.empty:
    print("No rows found.")
else:
    print(f"Retrieved {len(df)} rows:")
    print(df.to_string(index=False))