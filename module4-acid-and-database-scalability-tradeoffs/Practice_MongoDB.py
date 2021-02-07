
import pymongo
import os
from dotenv import load_dotenv
from pprintpp import pprint
import sqlite3
import pandas as pd
from numpy import NaN

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


#Connect to SQLite
# DB_FILEPATH = os.getenv("SQLITE_FILEPATH", default="OOPS")
DB_FILEPATH = os.path.join(os.path.dirname(__file__), "..", "module1-introduction-to-sql", "rpg_db.sqlite3")

connection_sqlite = sqlite3.connect(DB_FILEPATH)
# print('----------')
# print("CONNECTION SQLITE:", connection_sqlite)

cursor = connection_sqlite.cursor()
# print('----------')
# print("CURSOR:", cursor)

#pull the characters table from SQLite
query = '''SELECT * FROM charactercreator_character'''
rpg_df = pd.read_sql(query, connection_sqlite)

#deletes all the documents in the collection
collection.drop() 

# print("DOCS:", collection.count_documents({}))


#SQLite data concating/grouping all items/weapons for the same character
CHARACTER_ITEMS_LIST = '''SELECT character_id, group_concat(armory_item.name)  as items 
     FROM charactercreator_character_inventory JOIN armory_item on 
    charactercreator_character_inventory.item_id = armory_item.item_id GROUP BY character_id'''

CHARACTER_WEAPONS_LIST = '''SELECT character_id, group_concat(armory_item.name)  as weapons 
    FROM charactercreator_character_inventory 
    JOIN armory_item on charactercreator_character_inventory.item_id = armory_item.item_id 
    WHERE charactercreator_character_inventory.item_id in (select item_ptr_id from armory_weapon) 
    GROUP BY character_id'''


#converts each value from CHARACTER_ITEMS_LIST from one string to a list 
char_items_list = pd.read_sql(CHARACTER_ITEMS_LIST, connection_sqlite)
char_items_list['items'] = char_items_list['items'].apply(lambda x: x.split(','))

char_weapons_list = pd.read_sql(CHARACTER_WEAPONS_LIST, connection_sqlite)
# print(char_weapons_list.head())
char_weapons_list.weapons = char_weapons_list.weapons.apply(lambda x: x.split(','))

#merge together the df's character, items, weapons
final_df = rpg_df.merge(char_items_list, on='character_id',how='left').merge(char_weapons_list, on='character_id', how='left').drop(columns='character_id')

#replaces NaN's with nothing
final_df.weapons = final_df.weapons.apply(lambda x: [] if x is NaN else x)

#converts the dataframe to a dictionary, ready format for mongodb
char_records = final_df.to_dict('records')

#insert data to mongodb
collection.insert_many(char_records)


#Search for documents

#Total number of characters
print("Total characters:", collection.count_documents({}))

#total items
char_with_items = list(collection.find({'items': {"$exists": True}}))
nested_list_of_items = [character['items'] for character in char_with_items]
list_of_items = [item for character_items in nested_list_of_items for item in character_items]
print("Total items:", len(list_of_items))

# print("Total items:", db.armory_item.count_documents({}))

# print(list(collection.find())) #prints the entire collection
# for doc in collection.find({"name":"Aliquid iste optio reiciendi"}, {"$count":"items"}):
#     print(doc)

#How many total items?
# for number in collection.aggregate([{"$group": {"_id": "$items", "total" : {"$sum": "items"}}}]):
#     print(number)

# items_list = collection.find({"items":{}})
# total_items = items_list.count_documents()
# print("Total items:", total_items)

# print(collection.aggregate([{"$group":{"_id": "$items", "total": {"$sum": "$items"}}}]))


# collection.findOne({"name":"Aliquid iste optio reiciendi"}).items.length

#How many items per character? limit 20
# for doc in collection.aggregate([{"$group":{"_id":"$name", "total items": {"$sum": "items"}}}]).limit(20):
#     print("ITEMS BY CHARACTER:", doc.limit(20))
# collection.aggregate([{$group:{_id: "items", "count": {$sum: 1}}}])

# for doc in collection.aggregate([{"$group":{"_id":"$name", "average items": {"$avg": "$items"}}}]):
#     print("TOTAL ITEMS:", doc)


#THE OLD, INFERIOR WAY I DID IT
# rpg_characters = cursor.execute(query).fetchall()
# rpg_tuples = tuple(rpg_characters)

# def character_doc_creation(collection, rpg_characters): #applicable to titanic too, import as df
# #character - (id, name, level, exp, hp, strength, etc.)
#     for character in rpg_tuples:
#         character_doc = {
#         "name": character[1],
#         "level": character[2],
#         "exp": character[3],
#         "hp": character[4],
#         "strength": character[5],
#         "intelligence": character[6],
#         "dexterity": character[7],
#         "wisdom": character[8],
#         }
#         collection.insert_one(character_doc)

# character_doc_creation(collection, rpg_characters)

