                            Medallion Design with Astra DB

Overview:

    This project showcases Medallion Design with Cassandra on Astra DB.

Levels:

    Bronze Table: Raw data obtained from sales_100.csv

    Silver Table: Sales data after standardization and cleaning

Gold Tables:

    total revenue by product

    total revenue by country

    total revenue by sales channel
    
How to Run:
  1. Update the `linkedin-token.json` and secure bundle path
  2. Run: `python3 cassandra-medallion.py`