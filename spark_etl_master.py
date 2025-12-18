#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spark ETL for master_df.csv data.

Clean and transform master_df.csv and output as Parquet format.

Usage:
Local:
    spark-submit spark_etl_master.py --input data/master_df.csv --output output/master_parquet

Dataproc:
    gcloud dataproc jobs submit pyspark spark_etl_master.py \
      --cluster=<cluster-name> \
      --region=us-east1 \
      -- --input gs://bucket/data/master_df.csv \
         --output gs://bucket/output/master_parquet
"""


import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType


def build_spark(app_name: str = "MasterDataETL") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    return spark


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Input CSV path: data/master_df.csv or gs://bucket/data/master_df.csv"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Output Parquet path: output/master_parquet or gs://bucket/output/master_parquet"
    )
    return parser.parse_args()

def parse_list_column(col_name: str):
    # Convert a string-represented list to its first element
    return F.regexp_extract(F.col(col_name), r"\['([^']+)'\]", 1)

def parse_list_column_1(col_name: str):
    """
    Handles cases like:
    [64]              -> 64
    [63, 49]          -> 63
    ['NaN', 70, 67]   -> 70
    ['NaN']           -> null
    """
    return (
        F.expr(f"""
            element_at(
                filter(
                    split(
                        regexp_replace({col_name}, '\\\\[|\\\\]', ''),
                        ','
                    ),
                    x -> trim(regexp_replace(x, "'", "")) != 'NaN'
                         AND trim(regexp_replace(x, "'", "")) != ''
                ),
                1
            )
        """)
    )


def main():
    args = parse_args()
    spark = build_spark()

    print(f"Reading master_df.csv from: {args.input}")
    
    # Read CSV
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(args.input)
    
    print("Original schema:")
    df.printSchema()
    print(f"Total records: {df.count()}")

    # Data cleaning and transformation
    df_clean = (
        df
        # 1. Parse date columns
        .withColumn("event_date", F.to_date(F.col("date")))
        .withColumn("presale_start", F.to_timestamp(F.col("presale_date_start")))
        .withColumn("presale_end", F.to_timestamp(F.col("presale_date_end")))
        .withColumn("sale_start", F.to_timestamp(F.col("TM_sale_date_start")))
        
        # 2. Parse list-type columns (artists, spotify_followers, spotify_popularity)
        .withColumn("artist", parse_list_column("artists"))
        .withColumn("spotify_followers_clean", parse_list_column_1("spotify_followers"))
        .withColumn("spotify_popularity_clean", parse_list_column_1("spotify_popularity"))
        
        # 3. Type conversion - price columns
        .withColumn("tm_max_price", F.col("TM_max").cast(DoubleType()))
        .withColumn("tm_min_price", F.col("TM_min").cast(DoubleType()))
        .withColumn("sg_avg_price", F.col("SG_average_price").cast(DoubleType()))
        .withColumn("sg_min_price", F.col("SG_min_price").cast(DoubleType()))
        .withColumn("sg_max_price", F.col("SG_max_price").cast(DoubleType()))
        .withColumn("sh_max_price", F.col("SH_max_price").cast(DoubleType()))
        .withColumn("sh_min_price", F.col("SH_min_price").cast(DoubleType()))
        
        # 4. Type conversion - count columns
        .withColumn("sg_listing_count", F.col("SG_listing_count").cast(IntegerType()))
        .withColumn("sh_total_postings", F.col("SH_total_postings").cast(IntegerType()))
        .withColumn("sh_total_tickets", F.col("SH_total_tickets").cast(IntegerType()))
        
        # 5. Type conversion - Spotify columns
        .withColumn("spotify_followers", F.col("spotify_followers_clean").cast(IntegerType()))
        .withColumn("spotify_popularity", F.col("spotify_popularity_clean").cast(IntegerType()))
        
        # 6. Type conversion - geography columns
        .withColumn("venue_lat", F.col("TM_venue _lat").cast(DoubleType()))
        .withColumn("venue_long", F.col("TM_venue_long").cast(DoubleType()))
        
        # 7. Feature engineering
        .withColumn("price_range", F.col("tm_max_price") - F.col("tm_min_price"))
        .withColumn("has_spotify_data", F.col("spotify_followers").isNotNull())
        .withColumn("has_secondary_market", 
                    (F.col("sg_listing_count") > 0) | (F.col("sh_total_postings") > 0))
        
        # 8. Time-based features
        .withColumn("year", F.year("event_date"))
        .withColumn("month", F.month("event_date"))
        .withColumn("weekday", F.dayofweek("event_date"))
        .withColumn("is_weekend", F.col("weekday").isin([1, 7]))  # 1=Sunday, 7=Saturday
    )
    
    # Select final columns
    df_final = df_clean.select(
        # Basic info
        F.col("TM_id").alias("event_id"),
        F.col("event_title"),
        F.col("artist"),
        "event_date",
        
        # Venue 
        F.col("venue"),
        F.col("venue_city").alias("city"),
        F.col("venue_state").alias("state"),
        "venue_lat",
        "venue_long",
        
        # Classification
        F.col("genre"),
        F.col("subGenre").alias("subgenre"),
        F.col("event_type"),
        
        # Ticketmaster prices
        "tm_max_price",
        "tm_min_price",
        "price_range",
        
        # SeatGeek data
        "sg_avg_price",
        "sg_min_price",
        "sg_max_price",
        "sg_listing_count",
        F.col("SG_artists_score").cast(DoubleType()).alias("sg_artist_score"),
        F.col("SG_venue_score").cast(DoubleType()).alias("sg_venue_score"),
        
        # StubHub data
        "sh_max_price",
        "sh_min_price",
        "sh_total_postings",
        "sh_total_tickets",
        
        # Spotify data
        "spotify_followers",
        "spotify_popularity",
        "has_spotify_data",
        
        # Market features
        "has_secondary_market",
        
        # Time features
        "year",
        "month",
        "weekday",
        "is_weekend",
        
        # Sale dates
        "presale_start",
        "presale_end",
        "sale_start",
        
        # Others
        F.col("promoter"),
        F.col("span multiple days").alias("span_multiple_days")
    )

    # Data quality check
    print("\n Data Quality Check:")
    print(f"  Total records: {df_final.count()}")
    print(f"  Records with valid event_date: {df_final.filter(F.col('event_date').isNotNull()).count()}")
    print(f"  Records with artist: {df_final.filter(F.col('artist').isNotNull()).count()}")
    print(f"  Records with Spotify data: {df_final.filter(F.col('has_spotify_data')).count()}")
    print(f"  Records with secondary market: {df_final.filter(F.col('has_secondary_market')).count()}")

    # Deduplicate based on event_id
    df_dedup = df_final.dropDuplicates(["event_id"])
    print(f"\n After deduplication: {df_dedup.count()}")

    # Filter for required non-null fields
    df_filtered = df_dedup.filter(
        F.col("event_id").isNotNull() &
        F.col("event_date").isNotNull() &
        F.col("artist").isNotNull()
    )
    print(f"After filtering (event_id, event_date, artist not null): {df_filtered.count()}")

    # Show sample data
    print("\n Sample data:")
    df_filtered.show(5, truncate=False)

    # Write to Parquet (partitioned by year and month)
    print(f"\n Writing Parquet to: {args.output}")
    (
        df_filtered
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(args.output)
    )

    # Statistics
    print("\n Final Statistics:")
    df_filtered.select(
        F.count("*").alias("total_records"),
        F.countDistinct("artist").alias("unique_artists"),
        F.countDistinct("city").alias("unique_cities"),
        F.avg("tm_min_price").alias("avg_tm_price"),
        F.avg("spotify_popularity").alias("avg_spotify_popularity"),
        F.sum(F.when(F.col("has_secondary_market"), 1).otherwise(0)).alias("secondary_market_count")
    ).show()

    print("ETL complete.")
    spark.stop()


if __name__ == "__main__":
    main()