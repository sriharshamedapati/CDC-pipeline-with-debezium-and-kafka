CREATE TABLE IF NOT EXISTS products_replicated (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price_dollars DECIMAL(10,2) NOT NULL,
    source_created_at TIMESTAMPTZ
);