CREATE TABLE IF NOT EXISTS silver_sales (
    id UUID PRIMARY KEY,
    order_id TEXT,
    customer TEXT,
    amount DECIMAL,
    product TEXT,
    channel TEXT
);