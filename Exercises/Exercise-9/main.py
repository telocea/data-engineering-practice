import polars as pl

pl.Config.set_tbl_rows(-1)   # show all rows (-1 = unlimited)
pl.Config.set_tbl_cols(-1)  

def main():
    lf = pl.scan_csv("data/202306-divvy-tripdata.csv",
                     has_header=True,
                     schema_overrides={
                         "start_station_id": pl.Int64,
                         "end_station_id": pl.Int64
                     },
                     null_values=[""],
                     try_parse_dates=True,
                     infer_schema_length=1000,
                     )
    schema = lf.collect_schema()
    print(schema)

    q1 = num_bike_rides_per_day(lf)
    print(q1)

    q2 = week_analysis(lf)
    print(q2)

    q3 = week_comparison(lf)
    print(q3)


'''
Count the number bike rides per day.
'''
def num_bike_rides_per_day(lf):
    result = (
        lf
        .with_columns(
            pl.col('started_at').dt.date().alias('day')
        )
        .group_by('day')
        .agg([
            pl.col('ride_id').count().alias('num_rides')
        ])
        .sort('day')
    )
    df = result.collect()
    return df

'''
Calculate the average, max, and minimum number of rides per week of the dataset.
'''
def week_analysis(lf):
    weekly_counts = (
        lf
        .with_columns(
            pl.col('started_at').dt.truncate('1w').alias('week')
        )
        .group_by('week')
        .agg(
            pl.col('ride_id').count().alias('num_rides')
        )
        .sort('week')
    )

    summary = weekly_counts.select([
        pl.col('num_rides').max().alias("max_rides_per_week"),
        pl.col('num_rides').min().alias("min_rides_per_week"),
        pl.col('num_rides').mean().alias("avg_rides_per_week"),
    ])

    df = summary.collect()
    return df


'''
For each day, calculate how many rides that day is above or below the same day last week.
'''
def week_comparison(lf):
    daily = (
        lf.
        with_columns(pl.col('started_at').dt.date().alias('day'))
        .group_by('day')
        .agg(pl.col('ride_id').count().alias("num_rides"))
        .sort('day')
    )

    comparison = daily.with_columns(
        (pl.col('num_rides').cast(pl.Int64) - pl.col('num_rides').shift(7).cast(pl.Int64)).alias('diff')
    )

    df = comparison.collect()
    return df


if __name__ == "__main__":
    main()
