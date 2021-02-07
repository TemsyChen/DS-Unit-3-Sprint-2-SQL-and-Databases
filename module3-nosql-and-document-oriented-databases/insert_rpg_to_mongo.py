#What was easier, Postgres or MondoDB?
#They were both really difficult for me, maybe it was just the 
#learning curve of both that was tough for me.
#I prefer SQL, the logic of it is a little more straightforward,
#using cursor and SQL code.

import pymongo
import os
from dotenv import load_dotenv
from pprintpp import pprint
import sqlite3

load_dotenv()

#Connection to MongoDB
DB_USER = os.getenv("DB_USER", default="OOPS")
DB_PASSWORD = os.getenv("DB_PASSWORD", default="OOPS")
CLUSTER_NAME = os.getenv("CLUSTER_NAME", default="OOPS")
DB_NAME = os.getenv("DB_NAME", default="OOPS")

connection_uri = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@{CLUSTER_NAME}.mmkwb.mongodb.net/{DB_NAME}?retryWrites=true&w=majority"

# print("-----------")
# print("URI:", connection_uri)

client = pymongo.MongoClient(connection_uri)
# print("-----------")
# print("CLIENT:", type(client), client)

# #Create a database called RPG
db = client.rpg

# #Create a collection called character
collection = db.character
# print("----------")
# print("COLLECTION:", type(collection), collection)

# print("----------")
# print("COLLECTIONS:", db.list_collection_names)



#Insert one, just a test
# collection.insert_one({
#     "name": "Pikachu",
#     "level": 30,
#     "exp": 76000000000,
#     "hp": 400
# })
# print("DOCS:", collection.count_documents({}))


#Connect to SQLite
# DB_FILEPATH = os.getenv("SQLITE_FILEPATH", default="OOPS")
DB_FILEPATH = os.path.join(os.path.dirname(__file__), "..", "module1-introduction-to-sql", "rpg_db.sqlite3")

connection_sqlite = sqlite3.connect(DB_FILEPATH)
# print('----------')
# print("CONNECTION SQLITE:", connection_sqlite)

cursor = connection_sqlite.cursor()
# print('----------')
# print("CURSOR:", cursor)

#Join tables in SQLite to create a new table with the following instances
query = '''SELECT * FROM charactercreator_character'''

rpg_characters = cursor.execute(query).fetchall()
# rpg_tuples = tuple(rpg_characters)
# print("CHARACTER TABLE TYPE:", rpg_characters[0][1])


# #test insert one
# character_doc = {
#         "name": rpg_character[0][1],
#         "level": rpg_character[0][2],
#         "exp": rpg_character[0][3],
#         "hp": rpg_character[0][4],
#         "strength": rpg_character[0][5],
#         "intelligence": rpg_character[0][6],
#         "dexterity": rpg_character[0][7],
#         "wisdom": rpg_character[0][8],
#         }
# collection.insert_one(character_doc)
# print("DOCS:", collection.count_documents({}))

# collection.drop()

rpg_tuples = tuple(rpg_characters)

def character_doc_creation(collection, rpg_characters): #applicable to titanic too, import as df
#character - (id, name, level, exp, hp, strength, etc.)
    for character in rpg_tuples:
        character_doc = {
        "name": character[1],
        "level": character[2],
        "exp": character[3],
        "hp": character[4],
        "strength": character[5],
        "intelligence": character[6],
        "dexterity": character[7],
        "wisdom": character[8],
        }
        collection.insert_one(character_doc)

character_doc_creation(collection, rpg_characters)
print("DOCS:", collection.count_documents({}))

# #Create an items and weapons column in the new table
# CREATE TABLE rpg_characters AS
#   SELECT * FROM charactercreator_character;
# ALTER TABLE rpg_characters
#   ADD items varchar(50);

#TO DO
#join inventory items to rpg characters


# INNER JOIN (SELECT inventory.item_id FROM charactercreator_character_inventory AS inventory);

# INSERT INTO rpg_characters (items)
#   SELECT name FROM armory_item 
#   WHERE armory_item.item_id = rpg_characters.item_id;


# # collection.insert_many(rpg_characters)
# print(collection.count_documents({}))