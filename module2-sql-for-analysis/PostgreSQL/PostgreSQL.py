import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv("DB_NAME", default="OOPS")
DB_USER = os.getenv("DB_USER", default="OOPS")
DB_PASSWORD = os.getenv("DB_PASSWORD", default="OOPS")
DB_HOST = os.getenv("DB_HOST", default="OOPS")


connection = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)
print("connection", connection)


query = 'SELECT * from table_dspt9;'
cursor = connection.cursor()
print("CURSOR:", cursor)

table_name = "table_dspt9_2"

Query = f'''
CREATE TABLE IF NOT EXISTS {table_name} ( 
	Id SERIAL PRIMARY KEY,
	Name varchar(40) NOT NULL,
	Data JSONB
);
'''
print("SQL:", query)
Cursor.execute.query

connection.commit()

connection.close()