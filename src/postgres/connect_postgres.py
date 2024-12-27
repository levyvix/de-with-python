import psycopg2 as db

conn_string = "dbname='home' host='localhost' user='admin' password='l123'"

conn = db.connect(conn_string)
cur = conn.cursor()

# Correct usage of mogrify with parameterized queries
query = "insert into tabletop (name, id, street, city, zip) values (%s, %s, %s, %s, %s)"
params = ("big bird", 1, "sesame street", "fakeville", "12345")

# Use mogrify to generate a safely escaped query
safe_query = cur.mogrify(query, params)
print(safe_query)  # This prints the query with parameters safely escaped

cur.execute(safe_query)
conn.commit()
cur.close()
conn.close()
