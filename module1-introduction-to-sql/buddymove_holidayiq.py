import os
import sqlite3
import pandas as pd

DB_FILEPATH = os.path.join(os.path.dirname(__file__), "buddymove_holidayiq.sqlite3")

connection = sqlite3.connect(DB_FILEPATH)
print("CONNECTION", connection)

cursor = connection.cursor()
print("CURSOR", cursor)

#import the csv
df = pd.read_csv(r'C:\Users\temsy\Documents\GitHub\DS-Unit-3-Sprint-2-SQL-and-Databases\module1-introduction-to-sql\buddymove_holidayiq.csv')
# df = df.rename(columns={"User Id":"User_Id"})
df.columns = ((df.columns.str).replace(" ","_"))

df.to_sql('review', connection, if_exists='replace', index=False)

query = 'SELECT COUNT(User_Id) FROM review'
TOTAL_ROWS = cursor.execute(query).fetchone()[0]
print('TOTAL ROWS:', TOTAL_ROWS)

query = '''SELECT COUNT(User_Id) FROM review
WHERE Shopping > 100
AND Nature > 100'''
GOOD_REVIEWERS = cursor.execute(query).fetchone()[0]
print('USERS REVIEWED NATURE AND SHOPPING WELL:', GOOD_REVIEWERS)

for col in df.columns[1:]:
    query = 'SELECT AVG('+col+') from review'
    AVG = cursor.execute(query).fetchone()[0]
    print(f'{col} AVG:', AVG)

