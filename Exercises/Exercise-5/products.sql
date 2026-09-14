CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    product_code INTEGER NOT NULL,
    product_description VARCHAR(255) NOT NULL,
    UNIQUE (product_id, product_code, product_description)
);

CREATE INDEX IF NOT EXISTS idx_product_code ON products(product_code);