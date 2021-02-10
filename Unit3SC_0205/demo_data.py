import sqlite3


# Connect to the sqlite3 file

connection = sqlite3.connect("demo_data.sqlite3")

cursor = connection.cursor()


query = f""" DROP TABLE IF EXISTS demo """

cursor.execute(query)

# Create table
query = f"""
             CREATE TABLE IF NOT EXISTS demo (
             s varchar(10) PRIMARY KEY,
             x INT,
             y INT
         );
         """

cursor.execute(query)

# Insert data into table
insertion_query = f"""INSERT INTO demo VALUES
                  ('g', 3, 9),
                  ('v', 5, 7),
                  ('f', 9, 7);
                  """

cursor.execute(insertion_query)

# Queries
query1 = f""" SELECT COUNT(s) FROM demo"""
answer1 = cursor.execute(query1).fetchone()[0]
print("How many rows?", answer1)

query2 = f"""SELECT * FROM demo WHERE x >= 5 AND y >= 5"""
answer2 = cursor.execute(query2).fetchall()
print("Rows where x and y are at least 5:", answer2)

query3 = f"""SELECT COUNT(DISTINCT y) FROM demo"""
answer3 = cursor.execute(query3).fetchone()[0]
print("How many unique y values?", answer3)


connection.commit()

connection.close()
