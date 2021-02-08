
import pymongo
import os
from dotenv import load_dotenv
from pprintpp import pprint
import sqlite3
import pandas as pd
from numpy import NaN

load_dotenv()

# Connection to MongoDB
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


# Connect to SQLite
# DB_FILEPATH = os.getenv("SQLITE_FILEPATH", default="OOPS")
DB_FILEPATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "module1-introduction-to-sql",
    "rpg_db.sqlite3")

connection_sqlite = sqlite3.connect(DB_FILEPATH)
# print('----------')
# print("CONNECTION SQLITE:", connection_sqlite)

cursor = connection_sqlite.cursor()
# print('----------')
# print("CURSOR:", cursor)

# pull the characters table from SQLite
query = '''SELECT * FROM charactercreator_character'''
rpg_df = pd.read_sql(query, connection_sqlite)

# deletes all the documents in the collection
collection.drop()

# print("DOCS:", collection.count_documents({}))


# SQLite data concating/grouping all items/weapons for the same character
CHARACTER_ITEMS_LIST = '''SELECT character_id, group_concat(armory_item.name)  as items
     FROM charactercreator_character_inventory JOIN armory_item on
    charactercreator_character_inventory.item_id = armory_item.item_id GROUP BY character_id'''

CHARACTER_WEAPONS_LIST = '''SELECT character_id, group_concat(armory_item.name)  as weapons
    FROM charactercreator_character_inventory
    JOIN armory_item on charactercreator_character_inventory.item_id = armory_item.item_id
    WHERE charactercreator_character_inventory.item_id in (select item_ptr_id from armory_weapon)
    GROUP BY character_id'''


# converts each value from CHARACTER_ITEMS_LIST from one string to a list
char_items_list = pd.read_sql(CHARACTER_ITEMS_LIST, connection_sqlite)
char_items_list['items'] = char_items_list['items'].apply(
    lambda x: x.split(','))

char_weapons_list = pd.read_sql(CHARACTER_WEAPONS_LIST, connection_sqlite)
# print(char_weapons_list.head())
char_weapons_list.weapons = char_weapons_list.weapons.apply(
    lambda x: x.split(','))

# merge together the df's character, items, weapons
final_df = rpg_df.merge(
    char_items_list,
    on='character_id',
    how='left').merge(
        char_weapons_list,
        on='character_id',
        how='left').drop(
            columns='character_id')

# replaces NaN's with nothing
final_df.weapons = final_df.weapons.apply(lambda x: [] if x is NaN else x)

# converts the dataframe to a dictionary, ready format for mongodb
char_records = final_df.to_dict('records')

# insert data to mongodb
collection.insert_many(char_records)


# Search for documents

# Total number of characters
print("Total characters:", collection.count_documents({}))
#Total characters: 302

# total items
char_with_items = list(collection.find({'items': {"$exists": True}}))
nested_list_of_items = [character['items'] for character in char_with_items]

list_of_items = []
for sublist in nested_list_of_items:
    for item in sublist:
        list_of_items.append(item)

# list_of_items = [item for character_items in nested_list_of_items for item in character_items]
print("Total items:", len(list_of_items))
# lists 920 total items, answer should be 898


# How many items per character? limit 20
items_per_char = collection.aggregate(
    [{"$project": {"_id": 0, "name": 1, "item_count": {"$size": "$items"}}}])
print("ITEMS BY CHARACTER:")
pprint(list(items_per_char)[:20])

# ITEMS BY CHARACTER:
# [
#     {'item_count': 3, 'name': 'Aliquid iste optio reiciendi'},
#     {'item_count': 3, 'name': 'Optio dolorem ex a'},
#     {'item_count': 2, 'name': 'Minus c'},
#     {'item_count': 4, 'name': 'Sit ut repr'},
#     {'item_count': 4, 'name': 'At id recusandae expl'},
#     {'item_count': 1, 'name': 'Non nobis et of'},
#     {'item_count': 5, 'name': 'Perferendis'},
#     {'item_count': 3, 'name': 'Accusantium amet quidem eve'},
#     {'item_count': 4, 'name': 'Sed nostrum inventore error m'},
#     {'item_count': 4, 'name': 'Harum repellendus omnis od'},    
#     {'item_count': 3, 'name': 'Itaque ut commodi,'},
#     {'item_count': 3, 'name': 'Molestiae quis'},
#     {'item_count': 4, 'name': 'Ali'},
#     {'item_count': 4, 'name': 'Tempora quod optio possimus il'},    {'item_count': 4, 'name': 'Sed itaque beatae pari'},        
#     {'item_count': 1, 'name': 'Quam dolor'},
#     {'item_count': 5, 'name': 'Molestias expedita'},
#     {'item_count': 5, 'name': 'Lauda'},
#     {'item_count': 3, 'name': 'Incidunt sint perferen'},        
#     {'item_count': 1, 'name': 'Laboriosa'},
# ]

# How many of the items are weapons?
# total weapons
# returns every document with weapons
char_with_weapons = list(collection.find({'weapons': {'$exists': True}}))
nested_list_of_weapons = [character['weapons']
                          for character in char_with_items]
list_of_weapons = []
for sublist in nested_list_of_weapons:
    for weapons in sublist:
        list_of_weapons.append(weapons)

print("Total weapons:", len(list_of_weapons))
# Total weapons: 211

# How many items are not weapons?
print("Number of items that are not weapons:",
      len(list_of_items) - len(list_of_weapons))
# Number of items that are not weapons: 709

# How many weapons per character? limit 20
weapons_per_char = collection.aggregate(
    [{"$project": {"_id": 0, "name": 1, "item_count": {"$size": "$weapons"}}}])
print("WEAPONS PER CHARACTER:")
pprint(list(weapons_per_char)[:20])

# WEAPONS PER CHARACTER:
# [
#     {'item_count': 0, 'name': 'Aliquid iste optio reiciendi'},
#     {'item_count': 0, 'name': 'Optio dolorem ex a'},
#     {'item_count': 0, 'name': 'Minus c'},
#     {'item_count': 0, 'name': 'Sit ut repr'},
#     {'item_count': 2, 'name': 'At id recusandae expl'},
#     {'item_count': 0, 'name': 'Non nobis et of'},
#     {'item_count': 1, 'name': 'Perferendis'},
#     {'item_count': 0, 'name': 'Accusantium amet quidem eve'},
#     {'item_count': 0, 'name': 'Sed nostrum inventore error m'},
#     {'item_count': 0, 'name': 'Harum repellendus omnis od'},
#     {'item_count': 1, 'name': 'Itaque ut commodi,'},
#     {'item_count': 0, 'name': 'Molestiae quis'},
#     {'item_count': 0, 'name': 'Ali'},
#     {'item_count': 0, 'name': 'Tempora quod optio possimus il'},    
#     {'item_count': 0, 'name': 'Sed itaque beatae pari'},
#     {'item_count': 0, 'name': 'Quam dolor'},
#     {'item_count': 0, 'name': 'Molestias expedita'},
#     {'item_count': 0, 'name': 'Lauda'},
#     {'item_count': 0, 'name': 'Incidunt sint perferen'},
#     {'item_count': 1, 'name': 'Laboriosa'},
# ]

# What's the average number of items per character?
avg_items = collection.aggregate(
    [{"$group": {"_id": 0, "item_avg": {"$avg": {"$size": "$items"}}}}])
print("Average items per character:", list(avg_items))
# Average items per character: [{'_id': 0, 'item_avg': 3.0463576158940397}]

# What's the average number of weapons per character?
avg_weapons = collection.aggregate(
    [{"$group": {"_id": 0, "weapons_avg": {"$avg": {"$size": "$weapons"}}}}])
print("Average weapons per character:", list(avg_weapons))
# Average weapons per character: [{'_id': 0, 'weapons_avg':
# 0.6986754966887417}]

# THE OLD, INFERIOR WAY I DID IT
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
