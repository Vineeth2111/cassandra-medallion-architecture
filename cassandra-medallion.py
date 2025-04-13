import pandas as pd
import uuid
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json

# -------------------- Connect to Astra DB --------------------
with open("linkedin-token.json") as f:
    secrets = json.load(f)

ASTRA_CLIENT_ID = secrets["clientId"]
ASTRA_CLIENT_SECRET = secrets["secret"]

cloud_config = {
    'secure_connect_bundle': './secure-connect-linkedin.zip'
}

auth_provider = PlainTextAuthProvider(ASTRA_CLIENT_ID, ASTRA_CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

print("\n✅ Connected to Astra DB!")

# -------------------- Set Keyspace --------------------
session.set_keyspace("linkedin")

# -------------------- Load CSV --------------------
df = pd.read_csv('sales_100.csv')
print(f"\U0001f4be Loaded CSV with columns: {list(df.columns)}")

# -------------------- Bronze Table --------------------
session.execute("""
CREATE TABLE IF NOT EXISTS bronze_sales (
    id UUID PRIMARY KEY,
    order_id TEXT,
    customer TEXT,
    amount DECIMAL,
    product TEXT,
    channel TEXT
);
""")

for _, row in df.iterrows():
    session.execute("""
        INSERT INTO bronze_sales (id, order_id, customer, amount, product, channel)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        uuid.uuid4(),
        str(row['Order ID']),
        row['Country'],
        float(row['TotalRevenue']),
        row['Item Type'],
        row['Sales Channel']
    ))

print("✅ Bronze table loaded with raw data.")

# -------------------- Silver Table --------------------
session.execute("""
CREATE TABLE IF NOT EXISTS silver_sales (
    id UUID PRIMARY KEY,
    order_id TEXT,
    customer TEXT,
    amount DECIMAL,
    product TEXT,
    channel TEXT
);
""")

bronze_rows = session.execute("SELECT * FROM bronze_sales")

for row in bronze_rows:
    session.execute("""
        INSERT INTO silver_sales (id, order_id, customer, amount, product, channel)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        uuid.uuid4(),
        row.order_id,
        row.customer,
        row.amount,
        row.product,
        row.channel
    ))

print("✅ Silver table transformed and populated.")

# -------------------- Gold Table 1: Product Revenue --------------------
session.execute("""
CREATE TABLE IF NOT EXISTS gold_product_sales (
    product TEXT PRIMARY KEY,
    total_amount DECIMAL
);
""")

products = set(row.product for row in session.execute("SELECT product FROM silver_sales ALLOW FILTERING"))

for product in products:
    total = session.execute("""
        SELECT SUM(amount) FROM silver_sales WHERE product=%s ALLOW FILTERING
    """, [product]).one()[0]

    if total:
        session.execute("""
            INSERT INTO gold_product_sales (product, total_amount)
            VALUES (%s, %s)
        """, (product, total))

print("✅ Gold Table 1 (Product-wise) populated.")

# -------------------- Gold Table 2: Country Revenue --------------------
session.execute("""
CREATE TABLE IF NOT EXISTS gold_country_sales (
    country TEXT PRIMARY KEY,
    total_revenue DECIMAL
);
""")

countries = set(row.customer for row in session.execute("SELECT customer FROM silver_sales ALLOW FILTERING"))

for country in countries:
    total = session.execute("""
        SELECT SUM(amount) FROM silver_sales WHERE customer=%s ALLOW FILTERING
    """, [country]).one()[0]

    if total:
        session.execute("""
            INSERT INTO gold_country_sales (country, total_revenue)
            VALUES (%s, %s)
        """, (country, total))

print("✅ Gold Table 2 (Country-wise) populated.")

# -------------------- Gold Table 3: Channel Revenue --------------------
session.execute("""
CREATE TABLE IF NOT EXISTS gold_channel_sales (
    channel TEXT PRIMARY KEY,
    total_revenue DECIMAL
);
""")

channels = set(row.channel for row in session.execute("SELECT channel FROM silver_sales ALLOW FILTERING"))

for channel in channels:
    total = session.execute("""
        SELECT SUM(amount) FROM silver_sales WHERE channel=%s ALLOW FILTERING
    """, [channel]).one()[0]

    if total:
        session.execute("""
            INSERT INTO gold_channel_sales (channel, total_revenue)
            VALUES (%s, %s)
        """, (channel, total))

print("✅ Gold Table 3 (Channel-wise) populated.\n")
print("🎉 Medallion architecture flow completed successfully!")
