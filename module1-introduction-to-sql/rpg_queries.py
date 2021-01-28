import os
import sqlite3

# DB_FILEPATH = "./rpg_db.sqlite3"
DB_FILEPATH = os.path.join(os.path.dirname(__file__), "rpg_db.sqlite3")

connection = sqlite3.connect(DB_FILEPATH)
print("CONNECTION:", connection)

cursor = connection.cursor()
print("CURSOR", cursor)

#TOTAL_CHARACTERS: How many total Characters are there?
query = "SELECT count(name) as character_count FROM charactercreator_character;"
TOTAL_CHARACTERS = cursor.execute(query).fetchone()[0]
print("TOTAL_CHARACTERS", TOTAL_CHARACTERS)

#TOTAL_SUBCLASS: How many of each specific subclass?

subclasses = ['cleric','fighter','mage','necromancer','thief']
for subclass in subclasses:
    if subclass == 'necromancer':
        query = 'SELECT count(mage_ptr_id) FROM charactercreator_'+subclass+''
    else:
        query = 'SELECT count(character_ptr_id) FROM charactercreator_'+subclass+''
    TOTAL_SUBCLASS = cursor.execute(query).fetchall()[0]
    print(f'{subclass}:', TOTAL_SUBCLASS[0])

#TOTAL_ITEMS: How many total Items?
query = "SELECT count(name) as item_count From armory_item"
TOTAL_ITEMS = cursor.execute(query).fetchone()[0]
print("TOTAL ITEMS", TOTAL_ITEMS)

#WEAPONS: How many of the Items are weapons?
query = "SELECT count(item_ptr_id) as weapon_count From armory_weapon"
TOTAL_WEAPONS = cursor.execute(query).fetchone()[0]
print("TOTAL WEAPONS", TOTAL_WEAPONS)

#NON_WEAPONS: How many of the items are not weapons?
query = '''SELECT count(item_id) from armory_item 
WHERE item_id NOT IN (select item_ptr_id from armory_weapon)'''
NON_WEAPONS = cursor.execute(query).fetchone()[0]
print("NON_WEAPONS", NON_WEAPONS)

#CHARACTER_ITEMS: How many Items does each character have? (Return first 20 rows)
query = '''SELECT character_id, count(item_id) 
FROM charactercreator_character_inventory 
GROUP BY character_id LIMIT 20'''
CHARACTER_ITEMS = cursor.execute(query).fetchall()
print("CHARACTER ITEMS", CHARACTER_ITEMS)

#CHARACTER_WEAPONS: How many Weapons does each character have? (Return first 20 rows)
query = '''SELECT character_id, count(item_id) 
FROM charactercreator_character_inventory
WHERE item_id IN (SELECT item_ptr_id From armory_weapon)
GROUP BY character_id
LIMIT 20'''
CHARACTER_WEAPONS = cursor.execute(query).fetchall()
print('CHARACTER WEAPONS', CHARACTER_WEAPONS)

#AVG_CHARACTER_ITEMS: On average, how many Items does each Character have?
query = '''SELECT
round(avg(num_items), 2)
FROM(SELECT character_id
     ,count(item_id) as num_items
     FROM charactercreator_character_inventory
     GROUP BY character_id)'''
AVG_CHARACTER_ITEMS = cursor.execute(query).fetchone()[0]
print('AVG CHARACTER ITEMS', AVG_CHARACTER_ITEMS)

#AVG_CHARACTER_WEAPONS: On average, how many Weapons does each character have?
query = '''SELECT
round(avg(num_items), 2)
FROM(SELECT character_id
     ,count(item_id) as num_items
     FROM charactercreator_character_inventory
	 WHERE item_id IN (SELECT item_ptr_id From armory_weapon)
     GROUP BY character_id)'''
AVG_CHARACTER_WEAPONS = cursor.execute(query).fetchone()[0]
print('AVG CHARACTER WEAPONS', AVG_CHARACTER_WEAPONS)