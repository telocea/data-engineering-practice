from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import zipfile

def main():
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()
    # your code here
    file_name = 'hard-drive-2022-01-01-failures.csv.zip'

    with zipfile.ZipFile(f"data/{file_name}") as z:
        real_files = [
            f for f in z.namelist()
            if not f.startswith('__MACOSX') and f.endswith('.csv')
        ]
        z.extractall("extracted/", members=real_files)
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("extracted/")

    df.printSchema()
    
    df = add_source_file(df, file_name)
    df = add_file_date(df)
    df = add_brand(df)
    df = add_storage_ranking(df)
    df = add_primary_key(df)


    df.coalesce(1) \
    .write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(f"reports/path")

def add_source_file(df, file_name: str):
    # 1. Add the file name as a column to the DataFrame and call it `source_file`.
    df = df.withColumn("source_file", F.lit(file_name))
    return df


def add_file_date(df):
     # 2. Pull the `date` located inside the string of the `source_file` column. Final data-type must be 
    # `date` or `timestamp`, not a `string`. Call the new column `file_date`.
    df = df.withColumn(
        "file_date",
        F.to_date(
            F.regexp_extract(F.col("source_file"), r'(\d{4}-\d{2}-\d{2})', 1),
            'yyyy-MM-dd'
        )
    )
    return df


def add_brand(df):
    # 3. Add a new column called `brand`. It will be based on the column `model`. If the
    # column `model` has a space ... aka ` ` in it, split on that `space`. The value
    #    found before the space ` ` will be considered the `brand`. If there is no
    #    space to split on, fill in a value called `unknown` for the `brand`.
    df = df.withColumn(
        "brand",
        F.when(F.col("model").contains(" "), F.split(F.col("model"), " ").getItem(0))
        .otherwise('unknown')
    )
    return df
    


def add_storage_ranking(df):
    # 4. Inspect a column called `capacity_bytes`. Create a secondary DataFrame that
    # relates `capacity_bytes` to the `model` column, create "buckets" / "rankings" for
    #    those models with the most capacity to the least. Bring back that 
    #    data as a column called `storage_ranking` into the main dataset.
    avg_df = (
        df.groupBy('model')
          .agg(
              F.avg('capacity_bytes').alias('avg_capacity')
          )
    )
    
    window = Window.orderBy(F.desc("avg_capacity")) # window on aggregated result

    df = df.join(avg_df.withColumn("storage_ranking", F.rank().over(window)), on="model")

    return df


def add_primary_key(df):
    # 5. Create a column called `primary_key` that is `hash` of columns that make a record umique
    # in this dataset.
    key_columns = ["date", "serial_number", "model", "capacity_bytes", "failure"]
    df = df.withColumn(
        "primary_key",
        F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in key_columns]), 256)
    )

    return df

if __name__ == "__main__":
    main()
