import psycopg2 as db
from faker import Faker

fake = Faker()
data = []
i = 2
for _ in range(1000):
    data.append(
        (
            i,
            fake.name(),
            fake.street_address(),
            fake.city(),
            fake.zipcode(),
        )
    )
    i += 1


conn_string = "dbname='home' host='localhost' user='admin' password='l123'"

conn = db.connect(conn_string)
cur = conn.cursor()


# Correct usage of mogrify with parameterized queries
query = "insert into tabletop (id, name, street, city, zip) values (%s, %s, %s, %s, %s)"


# params = ("big bird", 1, "sesame street", "fakeville", "12345")

data_for_db = tuple(data)


cur.executemany(query, data_for_db)

conn.commit()
cur.close()
conn.close()
