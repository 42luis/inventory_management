# inventory_management
Reducing Costs by Optimizing Inventory

An Inventory Management Project, analysing data through an SQL + Python + Power BI ETL Pipeline, answering:
Purchasing products based on their Minimum Stock Level, instead of the current orders system, would represent less costs, and more monetary liquidity, to this retail store?

Data Source: https://www.kaggle.com/

Dataset: "Retail Store Inventory Forecasting Dataset" (by Anirudh Singh Chauhan)

ETL:
Extract - from a CSV dataset to perform Data Modeling into a Star Schema (or Galaxy Schema considering the Fact Constellation typology) using SQL (sqlite3)
    Creating dimension tables (product; store; calendar) and fact tables (sales; orders; inventory)

Transform - with Python for Data Analysis

Load - into Power BI for Business Intelligence