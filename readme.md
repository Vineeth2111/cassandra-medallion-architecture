Medallion Architecture Project with Astra DB:

This project undertakes a practical implementation of Medallion Architecture on the DataStax Astra DB with Python. It showcases how raw data can be processed into the various stages of Bronze, Silver, and Gold layer which is the essence of modern data lakehouse architecture.

---

What this project does:

- Imports sales data from a sample CSV file.
  
- Loads the raw data into a Bronze table (base level).
  
- Cleans and transforms data into the Silver table (refined level).
  
- Gold tables with insights include:
 
  - Total sales aggregated by Product.
  
  - Total sales aggregated by Country. 
  
  - Total sales aggregated by Sales Channel.

--- 

Tech Stack:

- DataStax's Astra DB (serverless Cassandra) 

- Git for version control
  
- Pandas for data manipulation
  
- An up to date version of Python 3.10+

- Cassanda driver for connecting to Astra DB

-----

Project Folder Structure:

cassandra-medallion-architecture/
├── cassandra-medallion.py         # Master script to execute the entire pipeline
├── connection.py                  # Manages Astra DB connection establishment
├── sales_100.csv                  # Input sales dataframe
├── secure-connect-linkedin.zip    # Safe package for Astra DB
├── linkedin-token.json            # Auth token (not committed)
├── readme.md                      # Project doc (this file)
│
├── cql/
│   ├── bronze.cql                 # CQL for Bronze table
│   ├── silver.cql                 # CQL for Silver table
│   └── gold.cql                   # Gold tables' CQL
│

└── screenshots/
    ├── gold_table1.png            # Sales output by products
    ├── gold_table2.png            # Sales output by country
└── gold_table3.png            # Channel-wise sales output

--------

Running This Project:

1. Clone this repository:
   git clone https://github.com/Vineeth2111/cassandra-medallion-architecture.git
   cd cassandra-medallion-architecture

2. Install dependencies:
   pip install pandas cassandra-driver

3. Make sure these files are in the folder:
   - sales_100.csv – your data
   - secure-connect-linkedin.zip – Astra secure bundle
   - linkedin-token.json – your Astra DB token

4. Run the pipeline:
   python cassandra-medallion.py

5. When done, the output will be:
   - ✅ Bronze, Silver, and Gold tables created and populated
   - ✅ Saved screenshots for all Gold tables at /screenshots

------

Why Medallion Architecture?:

This pattern helps to organize data pipelines into concrete layers:

| Layer   | Purpose                      |
|---------|------------------------------|
| Bronze  | Raw data (straight from source) |
| Silver  | Cleaned, organized data      |
| Gold    | Aggregated business insights  |

This design is scalable, easy to debug, and widely used in data engineering.

------

Gold Table Outputs:

Screenshots of the resulting SELECT * queries from the Gold tables are located in the screenshots/ directory — convenient for documentation or demo purposes.

---

Note on Security:

- The sensitive credentials-contained linkedin-token.json file has been excised from Git history and moved to.gitignore
- The GitHub Push Protection feature has been activated to prevent unauthorized token pushes inadvertently
