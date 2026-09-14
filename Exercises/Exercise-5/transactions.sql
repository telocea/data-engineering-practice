CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(50) PRIMARY KEY ,
    transaction_date DATE NOT NULL,
    product_id INTEGER NOT NULL,
    product_code INTEGER NOT NULL,
    product_description VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    account_id INTEGER,
    CONSTRAINT fk_transactions_account
        FOREIGN KEY (account_id) REFERENCES accounts(customer_id),
    CONSTRAINT fk_transactions_product
        FOREIGN KEY (product_id, product_code, product_description) REFERENCES products(product_id, product_code, product_description)
    
);

CREATE INDEX IF NOT EXISTS idx_transaction_date ON transactions(transaction_date);