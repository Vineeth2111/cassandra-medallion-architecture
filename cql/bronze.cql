CREATE TABLE IF NOT EXISTS bronze_sales (
    id UUID PRIMARY KEY,
    order_id TEXT,
    customer TEXT,
    amount DECIMAL,
    product TEXT,
    channel TEXT
);
