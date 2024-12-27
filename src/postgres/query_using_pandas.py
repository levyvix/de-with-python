import pandas as pd
import psycopg2 as db

conn_string = "dbname='home' host='localhost' user='admin' password='l123'"

conn = db.connect(conn_string)
cur = conn.cursor()

query = "select * from tabletop"

df_db = pd.read_sql(query, conn)

print(df_db.head())

print(df_db.head().to_json(orient="records"))

conn.close()
cur.close()
