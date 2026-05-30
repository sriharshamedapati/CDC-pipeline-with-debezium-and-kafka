CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price_cents INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO products (name, price_cents)
VALUES ('Sample Product', 10000);