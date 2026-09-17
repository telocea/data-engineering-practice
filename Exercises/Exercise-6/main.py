from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os
import zipfile


def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    # your code here
    # read data
    with zipfile.ZipFile("data/Divvy_Trips_2019_Q4.zip") as z:
        real_files = [
            f for f in z.namelist()
            if not f.startswith('__MACOSX') and f.endswith('.csv')
        ]
        z.extractall("extracted/", members=real_files)
    
    df_2019 = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("extracted/Divvy_Trips_2019_Q4.csv")
    
    os.makedirs('reports', exist_ok=True)

    report(1, get_average_dur(df_2019)) 
    report(2, get_num_trips(df_2019))
    report(3, get_pop_station(df_2019))
    report(4, get_top_three(df_2019))
    report(5, get_avg_gender_length(df_2019))
    report(6, get_longest_age(df_2019))

    spark.stop()


def get_average_dur(df):
    # 1. What is the `average` trip duration per day?
    avg_dur_day = (
        df.withColumn("year", F.year("start_time"))
          .withColumn("month", F.month("start_time"))
          .withColumn("day", F.dayofmonth("start_time"))
          .groupBy("year", "month", "day")
          .agg(
              F.avg("tripduration").alias("avg_trip_dur")
          )
          .orderBy("year", "month", "day")
    )
    return avg_dur_day


def get_num_trips(df):
    # 2. How many trips were taken each day?
    num_trips = (
        df.withColumn("year", F.year("start_time"))
            .withColumn("month", F.month("start_time"))
            .withColumn("day", F.dayofmonth("start_time"))
            .groupBy("year", "month", "day")
            .agg(
                F.count("*").alias("number_of_trips")
            )
            .orderBy("year", "month", "day")
    )
    return num_trips


def get_pop_station(df):
    # 3. What was the most popular starting trip station for each month?
    counted = (
        df.withColumn("month", F.month("start_time"))
          .groupBy("month", "from_station_name")
          .agg(
              F.count("*").alias("number_of_trips")
          )
    )
    window = Window.partitionBy("month").orderBy(F.desc("number_of_trips"))

    pop_station = (
        counted.withColumn("rank", F.row_number().over(window))
               .filter(F.col("rank") == 1)
               .drop("rank")
    )

    return pop_station


def get_top_three(df):
    # 4. What were the top 3 trip stations each day for the last two weeks?
    max_date = df.select(F.max("start_time")).collect()[0][0]

    # get from data
    from_two_weeks = (
         df.filter(F.col("start_time") >= F.date_sub(F.lit(max_date), 14))
           .select(F.col("from_station_name").alias("station"), F.col("start_time").alias("time"))
    )

    # get to data
    to_two_weeks = (
         df.filter(F.col("end_time") >= F.date_sub(F.lit(max_date), 14))
           .select(F.col("to_station_name").alias("station"), F.col("end_time").alias("time"))
    )
    # combine together
    two_weeks = from_two_weeks.unionByName(to_two_weeks)

    daily_counts = (
        two_weeks.withColumn("day", F.dayofmonth("time"))
                .groupBy("day", "station")
                .agg(
                    F.count("*").alias("num_visits")
                )
    )

    window = Window.partitionBy("day").orderBy(F.desc("num_visits"))

    result = (
        daily_counts.withColumn("rank", F.row_number().over(window))
                    .filter(F.col("rank") <= 3)
                    .orderBy("day", "rank")
    )

    return result


def get_avg_gender_length(df):
    # 5. Do `Male`s or `Female`s take longer trips on average?
    result = (
        df.groupBy("gender")
          .agg(
              F.avg("tripduration").alias("avg_dur")
          )
          .orderBy("avg_dur")
    )

    return result


def get_longest_age(df):
    # 6. What is the top 10 ages of those that take the longest trips, and shortest?
    # interpreted as top/bottom average time for each age group

    df = df.withColumn(
    "tripduration",
    F.regexp_replace(F.col("tripduration"), '"', '').cast("double")
    )
    
    df_clean = df.filter(
        (F.col("birthyear").isNotNull()) & (F.trim(F.col("birthyear")) != "") &
        (F.col("tripduration").isNotNull()) & (F.trim(F.col("tripduration")) != "")
    )
    
    year_dur = (
        df_clean.groupBy("birthyear")
          .agg(
              F.avg("tripduration").alias("avg_dur")
          )
    )

    top_ten = year_dur.orderBy(F.col("avg_dur").desc()).limit(10)
    bottom_ten = year_dur.orderBy(F.col("avg_dur").asc()).limit(10)

    result = top_ten.unionByName(bottom_ten)
    result = result.withColumn("age", F.year(F.current_date()) - F.col("birthyear")).drop("birthyear")
    
    return result


def report(q_num, answerdf):
    answerdf.coalesce(1) \
    .write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(f"reports/q{q_num}")

if __name__ == "__main__":
    main()
