import duckdb


def main():
    csv_path = "data/Electric_Vehicle_Population_Data.csv"
    db_path = "output.duckdb"

    con = duckdb.connect(db_path)

    # print("Creating table")
    # create_table(con)
    # print("Loading into table")
    # load_data(csv_path, con)

    count_cars_per_city(con)

    top_3_popular_vehicles(con)

    most_popular_vehicle_per_postal_code(con)

    count_by_model_year_to_parquet(con)


def create_table(con):
    con.sql(f"""
        CREATE TABLE electric_vehicles (
        vin VARCHAR,
        county VARCHAR,
        city VARCHAR,
        state VARCHAR,
        postal_code VARCHAR,
        model_year INTEGER,
        make VARCHAR,
        model VARCHAR,
        elec_vehicle_type VARCHAR,
        cafv_eligible BOOLEAN,
        electric_range INTEGER,
        base_msrp INTEGER,
        legislative_district INTEGER,
        dol_vehicle_id INTEGER UNIQUE,
        vehicle_loc DOUBLE[2],
        electric_utility VARCHAR,
        census_tract HUGEINT);
        """)



def load_data(csv_path: str, con) -> duckdb.DuckDBPyConnection:
    con.sql(f"""
        INSERT INTO electric_vehicles
        SELECT 
            "VIN (1-10)",
            County,
            City,
            State,
            "Postal Code",
            "Model Year",
            Make,
            Model,
            TRIM(STRING_SPLIT("Electric Vehicle Type", ' ')[-1], '()') AS elec_vehicle_type,
            CASE 
                WHEN "Clean Alternative Fuel Vehicle (CAFV) Eligibility" = 'Clean Alternative Fuel Vehicle Eligible' THEN TRUE
                WHEN "Clean Alternative Fuel Vehicle (CAFV) Eligibility" = 'Not eligible due to low battery range' THEN FALSE
                ELSE NULL
            END AS cafv_eligible,
            "Electric Range",
            "Base MSRP",
            "Legislative District",
            "DOL Vehicle ID",
            CAST([
                CAST(regexp_extract("Vehicle Location", 'POINT \(([-0-9.]+) ([-0-9.]+)\)', 1) AS DOUBLE),
                CAST(regexp_extract("Vehicle Location", 'POINT \(([-0-9.]+) ([-0-9.]+)\)', 2) AS DOUBLE)
            ] AS DOUBLE[2]) AS vehicle_loc,
            "Electric Utility",
            "2020 Census Tract"    
    FROM read_csv_auto("{csv_path}");
    """)

    row_count = con.sql(f"SELECT COUNT(*) FROM electric_vehicles").fetchone()[0]
    print(f"\nLoaded {row_count} rows into table 'electric_vehicles'.")



'''
Count the number of electric cars per city.
'''
def count_cars_per_city(con):
    cars_per_city = con.sql(f"""
        SELECT city AS City, COUNT(*) AS "Number of Cars"
        FROM electric_vehicles
        GROUP BY city
    """)
    print(cars_per_city)
    return cars_per_city
    


'''
Find the top 3 most popular electric vehicles.

Make and Model
'''
def top_3_popular_vehicles(con):
    top_3 = con.sql(f"""
        SELECT make, model
        FROM electric_vehicles
        GROUP BY make, model
        ORDER BY COUNT(*) DESC
        LIMIT 3
        """)

    print(top_3)
    return top_3



'''
Find the most popular electric vehicle in each postal code.
'''
def most_popular_vehicle_per_postal_code(con):

    popular_per_postal = con.sql(f"""
        WITH counted AS (
            SELECT
                postal_code,
                make,
                model,
                COUNT(*) AS occurrence_count
            FROM electric_vehicles
            GROUP BY postal_code, make, model
        )
        SELECT make, model, postal_code
        FROM 
            (SELECT
                make,
                model,
                postal_code,
                ROW_NUMBER() OVER(PARTITION BY postal_code ORDER BY occurrence_count DESC) as rnk
            FROM counted
            ) subquery
        WHERE rnk = 1
        ORDER BY postal_code
        """)

    print(popular_per_postal)
    return popular_per_postal


'''
Count the number of electric cars by model year. Write out the answer as parquet files partitioned by year.
'''
def count_by_model_year_to_parquet(con):
    number_by_model_year = con.sql(f"""
    COPY (
        SELECT COUNT(*), model_year
        FROM electric_vehicles
        GROUP BY model_year
    )
    to 'by_year_verify'
    (FORMAT PARQUET, PARTITION_BY (model_year), OVERWRITE_OR_IGNORE)
    """)

    # print(number_by_model_year)
    number_in_2023 = con.sql("SELECT * FROM 'by_year_verify/**/*.parquet' WHERE model_year = '2023';")
    print(number_in_2023)
    return number_by_model_year



if __name__ == "__main__":
    main()
