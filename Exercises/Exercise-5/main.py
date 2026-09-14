import psycopg2


def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas, port=5432)
    # your code 
    
    cur = conn.cursor()
    with open('accounts.sql', 'r') as f:
        sql_script = f.read()
        cur.execute(sql_script)

    with open('products.sql', 'r') as f:
        sql_script = f.read()
        cur.execute(sql_script)

    with open('transactions.sql', 'r') as f:
            sql_script = f.read()
            cur.execute(sql_script)

    with open("data/accounts.csv", "r") as f:
        cur.copy_expert(
        "COPY accounts (customer_id, first_name, last_name, address_1, address_2, city, state, zip_code, join_date) FROM STDIN WITH CSV HEADER",
        f
    )

    with open("data/products.csv", "r") as f:
        cur.copy_expert(
        "COPY products (product_id, product_code, product_description) FROM STDIN WITH CSV HEADER",
        f
    )

    with open("data/transactions.csv", "r") as f:
        cur.copy_expert(
        "COPY transactions (transaction_id, transaction_date, product_id, product_code, product_description, quantity, account_id) FROM STDIN WITH CSV HEADER",
        f
    )

    # cur.execute("SELECT * FROM transactions")
    # rows = cur.fetchall()
    # print(rows)

    conn.commit()

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
