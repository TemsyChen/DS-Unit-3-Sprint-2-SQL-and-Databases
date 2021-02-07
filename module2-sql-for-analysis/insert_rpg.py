import psycopg2
import os
import sqlite3
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv

load_dotenv()

#connecting to the rpg database in another folder
DB_FILEPATH = os.path.join(os.path.dirname(__file__), "..", "module1-introduction-to-sql", "rpg_db.sqlite3")

sql_connection = sqlite3.connect(DB_FILEPATH)

sql_cursor = sql_connection.cursor()

#fetch the characters table from the database in SQLite3
query = """SELECT*FROM charactercreator_character"""
characters = sql_cursor.execute(query).fetchall()

#Connect to ElephantSQL-hosted PostgreSQL
DB_NAME = os.getenv("DB_NAME", default="OOPS")
DB_USER = os.getenv("DB_USER", default="OOPS")
DB_PASSWORD = os.getenv("DB_PASSWORD", default="OOPS")
DB_HOST = os.getenv("DB_HOST", default="OOPS")

pg_connection = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)
print("CONNECTION:", pg_connection)

pg_cursor = pg_connection.cursor()
print("CURSOR:", pg_cursor)

#Create table on ElephantSQL
pg_query = f"""DROP TABLE RPG_character;
         CREATE TABLE IF NOT EXISTS RPG_character (
             character_id SERIAL PRIMARY KEY,
             name VARCHAR(40),
             level INT,
             exp INT,
             hp INT,
             strength INT,
             intelligence INT,
             dexterity INT,
             wisdom INT
         );
         """

pg_cursor.execute(pg_query)

#Insert table to PostgreSQL
insertion_query = f"""INSERT INTO RPG_character (character_id, name, level, 
                  exp, hp, strength, intelligence, dexterity, wisdom) 
                  VALUES %s"""

list_of_tuples = characters
execute_values(pg_cursor, insertion_query, list_of_tuples)

sql_connection.commit()

sql_connection.close()