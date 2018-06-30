import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Connect to the database
conn = psycopg2.connect(database="postgres", user="power_user", password="$poweruserpassword", host="localhost", port="5432")
cur = conn.cursor()
cur.execute("SELECT * from tweets")
records = cur.fetchall()
for rec in records:
   print(rec)
conn.commit()

conn.close()
