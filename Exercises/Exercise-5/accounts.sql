CREATE TABLE IF NOT EXISTS accounts (
    customer_id INTEGER PRIMARY KEY ,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    address_1 VARCHAR(255) NOT NULL,
    address_2 VARCHAR(255),
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100) NOT NULL,
    zip_code INTEGER NOT NULL,
    join_date DATE NOT NULL
);