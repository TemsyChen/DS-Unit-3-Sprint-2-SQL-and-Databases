import psycopg2
from psycopg2.extras import execute_values
import json
import pandas as pd
import csv
import os
from dotenv import load_dotenv


load_dotenv()

DBT_NAME =  os.getenv("DBT_NAME", default="OOPS")
DBT_USER =  os.getenv("DBT_USER", default="OOPS")
DBT_PASSWORD = os.getenv("DBT_PASSWORD", default="OOPS")
DBT_HOST = os.getenv("DBT_HOST", default="OOPS")

#Connect to ELephantSQL-hosted PostgreSQL
connection = psycopg2.connect(dbname=DBT_NAME, user=DBT_USER, 
                        password=DBT_PASSWORD, host=DBT_HOST)
print("CONNECTION", type(connection))

#Cursor to iterate over db records to perform queries
cursor = connection.cursor()
print("CURSOR", type(cursor))

# breakpoint()

# table_name = "titanic"
query = f"""
DROP TABLE titanic;
CREATE TABLE IF NOT EXISTS titanic (
   survived integer,
   pclass integer,
   name varchar(100),
   sex varchar(20),
   age float,
   siblings_spouses_aboard integer,
   parents_children_aboard integer,
   fare float
);
"""

print("SQL:", query)
cursor.execute(query)

#Test insert to titanic
# insertion_query = f'''INSERT INTO titanic (survived, pclass, name, 
#                     sex, age, siblings_spouses_aboard, parents_children_aboard, fare) 
#                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s);'''
# cursor.execute(insertion_query,
#    ('1','1','Test Name','Female','30','2','2','4.25')
#    )


#Import titanic csv and insert table to database

with open(r'C:\Users\temsy\Documents\GitHub\DS-Unit-3-Sprint-2-SQL-and-Databases\module2-sql-for-analysis\titanic.csv', 'r') as f:
   next(f)#skips the header row
   cursor.copy_from(f, 'titanic', sep=',')


#ANOTHER WAY TO DO THIS:
# f_contents = open(r'C:\Users\temsy\Documents\GitHub\DS-Unit-3-Sprint-2-SQL-and-Databases\module2-sql-for-analysis\titanic.csv', 'r')
# cursor.copy_from(f_contents, "titanic", columns=('survived', 'pclass', 'name', 'sex', age', 'siblings_spouses_aboard', 'parents_children_aboard'), sep=",")


#ANOTHER WAY TO DO THIS:
# insertion_query = f"INSERT INTO titanic (survived, pclass, name, sex, age, siblings_spouses_aboard, parents_children_aboard) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
# df = pd.read_csv(r'C:\Users\temsy\Documents\GitHub\DS-Unit-3-Sprint-2-SQL-and-Databases\module2-sql-for-analysis\titanic.csv')
# records = df.to_dict(titanic)
# list_of_tuples = [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]) for r in records]
# execute_values(cursor, insertion_query, list_of_tuples)

connection.commit()

connection.close()