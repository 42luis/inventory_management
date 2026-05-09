#Reducing Costs by Optimizing Inventory - an Inventory Management Project (Example for Portfolio Purposes Only)

import pandas as pd
import sqlite3 as sq
import numpy as np
from scripts.sql_calendar import create_dim_calendar

df = pd.read_csv("data/retail_store_inventory.csv")    #Load CSV Dataset

conn = sq.connect("data/star_schema.db")
cur = conn.cursor()

df.to_sql("single_dataset", conn, if_exists="replace", index=False)    #Import Single-table Dataset to the SQL Database

#Creating Dimensions Tables
    #Creating Product Dimensions Table

cur.execute("DROP TABLE IF EXISTS dim_product")

cur.execute("""CREATE TABLE IF NOT EXISTS dim_product (
            product_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_code TEXT UNIQUE,
            category TEXT
            )""")

#Populating Product Dimensions Table - considering "Product ID" repeats for diferent "Category", must concatenate those two dimensions to create a unique "product_code" for each product

cur.execute("""INSERT OR IGNORE INTO dim_product (
            product_code,
            category)
            SELECT DISTINCT "Category"||'_'||"Product ID",
            "Category"
            FROM single_dataset""")

#Creating Store Dimensions Table

cur.execute("DROP TABLE IF EXISTS dim_store")

cur.execute("""CREATE TABLE IF NOT EXISTS dim_store (
            store_id INTEGER PRIMARY KEY AUTOINCREMENT,
            store_code TEXT UNIQUE,
            region TEXT
            )""")

#Populating Store Dimensions Table - considering "Store ID" repeats for diferent "Region", must concatenate those two dimensions to create a unique "store_code" for each store

cur.execute("""INSERT OR IGNORE INTO dim_store (
            store_code,
            region)
            SELECT DISTINCT "Region"||'_'||"Store ID",
            "Region"
            FROM single_dataset""")

create_dim_calendar(conn) #Calling the Function that creates Calendar Dimensions Table stored on a Module (since it's created from scratch)

#Creating Fact Tables
    #Creating Sales Fact Table

cur.execute("DROP TABLE IF EXISTS fact_sales")
cur.execute("""CREATE TABLE IF NOT EXISTS fact_sales (
            sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            store_id INTEGER,
            product_id INTEGER,
            units_sold INTEGER,
            FOREIGN KEY (store_id) REFERENCES dim_store(store_id),
            FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
            FOREIGN KEY (date) REFERENCES dim_calendar(date_key)
            )""")

    #Populating Sales Fact Table

cur.execute("""INSERT INTO fact_sales (
            date, store_id, product_id, units_sold)
            SELECT c.date_key,
            s.store_id,
            p.product_id,
            d."Units Sold"
            FROM single_dataset d
            LEFT JOIN dim_store s ON (d."Region"||'_'||d."Store ID") = s.store_code
            LEFT JOIN dim_product p ON (d."Category"||'_'||d."Product ID") = p.product_code
            LEFT JOIN dim_calendar c ON d."Date" = c.date_key """)

    #Creating Orders Fact Table

cur.execute("DROP TABLE IF EXISTS fact_orders")
cur.execute("""CREATE TABLE IF NOT EXISTS fact_orders (
            order_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            store_id INTEGER,
            product_id INTEGER,
            units_ordered INTEGER,
            price FLOAT,
            FOREIGN KEY (store_id) REFERENCES dim_store(store_id),
            FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
            FOREIGN KEY (date) REFERENCES dim_calendar(date_key)
            )""")

    #Populating Orders Fact Table

cur.execute("""INSERT INTO fact_orders (
            date, store_id, product_id, units_ordered, price)
            SELECT c.date_key,
            s.store_id,
            p.product_id,
            d."Units Ordered",
            d."Price"
            FROM single_dataset d
            LEFT JOIN dim_store s ON (d."Region"||'_'||d."Store ID") = s.store_code
            LEFT JOIN dim_product p ON (d."Category"||'_'||d."Product ID") = p.product_code
            LEFT JOIN dim_calendar c ON d."Date" = c.date_key """)

    #Creating Inventory Fact Table

cur.execute("DROP TABLE IF EXISTS fact_inventory")
cur.execute("""CREATE TABLE IF NOT EXISTS fact_inventory (
            snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            store_id INTEGER,
            product_id INTEGER,
            inventory_units INTEGER,
            price FLOAT,
            FOREIGN KEY (store_id) REFERENCES dim_store(store_id),
            FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
            FOREIGN KEY (date) REFERENCES dim_calendar(date_key)
            )""")

    #Populating Inventory Fact Table

cur.execute("""INSERT INTO fact_inventory (
            date, store_id, product_id, inventory_units, price)
            SELECT c.date_key,
            s.store_id,
            p.product_id,
            d."Inventory Level",
            d."Price"
            FROM single_dataset d
            LEFT JOIN dim_store s ON (d."Region"||'_'||d."Store ID") = s.store_code
            LEFT JOIN dim_product p ON (d."Category"||'_'||d."Product ID") = p.product_code
            LEFT JOIN dim_calendar c ON d."Date" = c.date_key """)
    
#Validating Inventory Fact Table
 
cur.execute("""SELECT * FROM fact_inventory LIMIT 50""")
print(f"{cur.fetchall()}\n")

conn.commit()
conn.close()