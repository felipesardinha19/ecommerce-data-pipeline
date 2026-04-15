import os
import psycopg2

os.environ["PGCLIENTENCODING"] = "UTF8"

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="analytics",
    user="postgres",
    password="52543212"
)

print("Conectou!")