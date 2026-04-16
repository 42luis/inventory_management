import pandas as pd
import sqlite3 as sq

def create_dim_calendar(mod_conn):
    
    cur = mod_conn.cursor()

    #Creating Calendar Dimensions Table from scratch

    cur.execute("DROP TABLE IF EXISTS dim_calendar")
    cur.execute("""CREATE TABLE IF NOT EXISTS dim_calendar (
                date_key TEXT PRIMARY KEY,
                day INTEGER,
                month INTEGER,
                year INTEGER,
                month_name TEXT,
                season TEXT
                )""")

    #Populating Calendar Dimensions Table

        #Searching Calendar Limits

    df=pd.read_csv("data/retail_store_inventory.csv")
    df["Date"] = pd.to_datetime(df["Date"])
    first_date = df["Date"].min().strftime('%Y-%m-%d')
    last_date = df["Date"].max().strftime('%Y-%m-%d')

    cur.execute(f"""
    WITH RECURSIVE dates(date) AS (
        VALUES('{first_date}')
        UNION ALL
        SELECT date(date, '+1 day') FROM dates WHERE date < '{last_date}')
    INSERT INTO dim_calendar (date_key, day, month, year, month_name, season)
    SELECT
        date,
        CAST(strftime('%d', date) AS INTEGER),
        CAST(strftime('%m', date) AS INTEGER),
        CAST(strftime('%Y', date) AS INTEGER),
        CAST(CASE strftime('%m', date) WHEN '01' THEN 'JANUARY' WHEN '02' THEN 'FEBRUARY' WHEN '03' THEN 'MARCH' WHEN '04' THEN 'APRIL' WHEN '05' THEN 'MAY' WHEN '06' THEN 'JUNE' WHEN '07' THEN 'JULY' WHEN '08' THEN 'AUGUST' WHEN '09' THEN 'SEPTEMBER' WHEN '10' THEN 'OCTOBER' WHEN '11' THEN 'NOVEMBER' WHEN '12' THEN 'DECEMBER' END AS TEXT),
        CAST(CASE strftime('%m', date) WHEN '01' THEN 'WINTER' WHEN '02' THEN 'WINTER' WHEN '03' THEN 'SPRING' WHEN '04' THEN 'SPRING' WHEN '05' THEN 'SPRING' WHEN '06' THEN 'SUMMER' WHEN '07' THEN 'SUMMER' WHEN '08' THEN 'SUMMER' WHEN '09' THEN 'AUTUMN' WHEN '10' THEN 'AUTUMN' WHEN '11' THEN 'AUTUMN' WHEN '12' THEN 'WINTER' END AS TEXT)
    FROM dates""")

    mod_conn.commit()