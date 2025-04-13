-- Gold Table 1: Product-wise revenue
CREATE TABLE IF NOT EXISTS gold_product_sales (
    product TEXT PRIMARY KEY,
    total_amount DECIMAL
);

-- Gold Table 2: Country-wise revenue
CREATE TABLE IF NOT EXISTS gold_country_sales (
    country TEXT PRIMARY KEY,
    total_revenue DECIMAL
);

-- Gold Table 3: Channel-wise revenue
CREATE TABLE IF NOT EXISTS gold_channel_sales (
    channel TEXT PRIMARY KEY,
    total_revenue DECIMAL
);