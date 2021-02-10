import sqlite3


# Connect to the sqlite3 file

connection = sqlite3.connect("northwind_small.sqlite3")

cursor = connection.cursor()

# Queries
# `expensive_items`: What are the ten most expensive items (per unit price) in the database?
price_query = f""" SELECT UnitPrice, ProductName
                   FROM product
                   ORDER BY UnitPrice DESC
                   LIMIT 10;"""
expensive_items = cursor.execute(price_query).fetchall()
print("Expensive items:", expensive_items)
# Expensive items: [(263.5, 'Côte de Blaye'), (123.79, 'Thüringer Rostbratwurst'),
# (97, 'Mishi Kobe Niku'), (81, "Sir Rodney's Marmalade"), (62.5, 'Carnarvon Tigers'),
# (55, 'Raclette Courdavault'), (53, 'Manjimup Dried Apples'), (49.3, 'Tarte au sucre'),
# (46, 'Ipoh Coffee'), (45.6, 'Rössle Sauerkraut')]

# `avg_hire_age`: What is the average age of an employee at the time of their hiring?

# ONLY RAN THIS THE FIRST TIME, then commented it out
# add_age_column = f"""
#                 ALTER TABLE Employee
#                 ADD age INT AS (hiredate - birthdate)
#              """
# cursor.execute(add_age_column)

avghire_query = f"""SELECT AVG(age) from employee"""
avg_hire_age = cursor.execute(avghire_query).fetchone()[0]
print("Average hire age:", avg_hire_age)
# Average hire age: 37.22222222222222

# (*Stretch*) `avg_age_by_city`: How does the average age of employee at hire vary by city?
avg_by_city_query = f"""SELECT AVG(age), city FROM employee
                        GROUP BY city
                     """
avg_age_by_city = cursor.execute(avg_by_city_query).fetchall()
print("Average age by city:", avg_age_by_city)
# Average age by city: [(29.0, 'Kirkland'), (32.5, 'London'),
# (56.0, 'Redmond'), (40.0, 'Seattle'), (40.0, 'Tacoma')]

# - `ten_most_expensive`: What are the ten most expensive items (per unit price) in the database
# *and* their suppliers?

# COMMENTING OUT AFTER RUNNING ONCE
# suppliers_prices_table = f"""CREATE TABLE suppliers_prices AS
#                     SELECT Product.ProductName, Product.UnitPrice, Supplier.CompanyName
#                     FROM Product
#                     LEFT JOIN Supplier ON Product.SupplierId = Supplier.Id
#                     """
# cursor.execute(suppliers_prices_table)

# insertion_query = f"""SELECT Product.ProductName, Product.UnitPrice, Supplier.CompanyName
#                     FROM Product
#                     LEFT JOIN Supplier ON Product.SupplierId = Supplier.Id"""
# cursor.execute(insertion_query)

price_supplier_query = f"""SELECT unitprice, companyname
                           FROM suppliers_prices
                           ORDER BY unitprice DESC
                           LIMIT 10;
                        """
price_supplier_topten = cursor.execute(price_supplier_query).fetchall()
print("Top most expensive items and their suppliers:", price_supplier_topten)
# Top most expensive items and their suppliers: [(263.5, 'Aux
# joyeux ecclésiastiques'), (123.79, 'Plutzer Lebensmittelgroßmärkte AG'),
# (97, 'Tokyo Traders'), (81, 'Specialty Biscuits, Ltd.'),
# (62.5, 'Pavlova, Ltd.'), (55, 'Gai pâturage'), (53, "G'day, Mate"),
# (49.3, "Forêts d'érables"), (46, 'Leka Trading'), (45.6, 'Plutzer Lebensmittelgroßmärkte AG')]

# - `largest_category`: What is the largest category (by number of unique products in it)?
largest_category_query = f"""SELECT CategoryId, COUNT(DISTINCT ProductName) FROM Product
                       GROUP BY CategoryId
                       ORDER BY COUNT(DISTINCT ProductName) DESC"""
largest_category = cursor.execute(largest_category_query).fetchone()[0]
print("Largest category:", largest_category)
# Largest category: 3

# - (*Stretch*) `most_territories`: Who's the employee with the most territories?
# Use `TerritoryId` (not name, region, or other fields) as the unique
# identifier for territories.

# COMMENT OUT AFTER RUNNING ONCE
# employee_territory_table = f"""CREATE TABLE employee_territory AS
#                                SELECT Employee.FirstName, Employee.LastName,
#                                EmployeeTerritory.EmployeeId, EmployeeTerritory.TerritoryId
#                                FROM Employee
#                                JOIN EmployeeTerritory ON Employee.Id = EmployeeTerritory.EmployeeId;"""
# cursor.execute(employee_territory_table)

territory_query = f"""SELECT COUNT(DISTINCT TerritoryId), FirstName, LastName, EmployeeId from employee_territory
                      GROUP BY EmployeeId
                      ORDER BY COUNT(DISTINCT TerritoryId) DESC"""
employee_territory = cursor.execute(territory_query).fetchone()
print("Which employee has the most territory?", employee_territory)
# Which employee has the most territory? (10, 'Robert', 'King', 7)

connection.commit()

connection.close()
