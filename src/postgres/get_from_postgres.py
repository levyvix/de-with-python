import psycopg2 as db

conn_string = "dbname='home' host='localhost' user='admin' password='l123'"

conn = db.connect(conn_string)
cur = conn.cursor()


query = "select * from tabletop"

cur.execute(query)


with open("fromdb.csv", "w") as f:
    cur.copy_to(f, "tabletop", sep=",")

cur.close()
conn.close()
