#Reducing Costs by Optimizing Inventory - an Inventory Management Project (Example for Portfolio Purposes Only)

import pandas as pd
import sqlite3 as sq
import numpy as np

df = pd.read_csv("data/retail_store_inventory.csv")    #Load CSV Dataset

conn = sq.connect("data/star_schema.db")
cur = conn.cursor()

df.to_sql("single_dataset", conn, if_exists="replace", index=False)    #Import Single-table Dataset to the SQL Database

cur.execute("""CREATE TABLE IF NOT EXISTS dim_product (
            product_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_code TEXT
            )""")    #Creating Product Dimensions Table

cur.execute("""INSERT INTO dim_product (product_code) SELECT DISTINCT "Product ID" FROM single_dataset""")    #Populating Product Dimensions Table

cur.execute("""SELECT * FROM dim_product""")
print(cur.fetchall())    #Validating first steps

conn.commit()
conn.close()