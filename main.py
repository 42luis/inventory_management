#Reducing Costs by Optimizing Inventory - an Inventory Management Project (Example for Portfolio Purposes Only)

import pandas as pd
import sqlite3 as sq
import numpy as np
from scripts.sql_calendar import create_dim_calendar

df = pd.read_csv("data/retail_store_inventory.csv")    #Load CSV Dataset

conn = sq.connect("data/star_schema.db")
cur = conn.cursor()

df.to_sql("single_dataset", conn, if_exists="replace", index=False)    #Import Single-table Dataset to the SQL Database

#Creating Product Dimensions Table

cur.execute("DROP TABLE IF EXISTS dim_product")

cur.execute("""CREATE TABLE IF NOT EXISTS dim_product (
            product_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_code TEXT UNIQUE,
            category TEXT,
            list_price FLOAT
            )""")

#Populating Product Dimensions Table - considering "Product ID" repeats for diferent "Category", must concatenate those two dimensions to create a unique "product_code" for each product

cur.execute("""INSERT OR IGNORE INTO dim_product (product_code, category, list_price) SELECT DISTINCT "Category"||'_'||"Product ID", "Category", "Price" FROM single_dataset""")

#Creating Store Dimensions Table

cur.execute("DROP TABLE IF EXISTS dim_store")

cur.execute("""CREATE TABLE IF NOT EXISTS dim_store (
            store_id INTEGER PRIMARY KEY AUTOINCREMENT,
            store_code TEXT UNIQUE,
            region TEXT
            )""")

#Populating Store Dimensions Table - considering "Store ID" repeats for diferent "Region", must concatenate those two dimensions to create a unique "store_code" for each store

cur.execute("""INSERT OR IGNORE INTO dim_store (store_code, region) SELECT DISTINCT "Region"||'_'||"Store ID", "Region" FROM single_dataset""")

create_dim_calendar(conn) #Calling the creation of Calendar Dimensions Table
    
#Validating first steps
 
cur.execute("""SELECT * FROM dim_product""")
print(f"{cur.fetchall()}\n")

cur.execute("""SELECT * FROM dim_store""")
print(f"{cur.fetchall()}\n")

cur.execute("""SELECT * FROM dim_calendar LIMIT 10""")
print(f"{cur.fetchall()}\n")

cur.execute("""SELECT * FROM dim_calendar ORDER BY date_key DESC LIMIT 10""")
print(f"{cur.fetchall()}\n")


conn.commit()
conn.close()