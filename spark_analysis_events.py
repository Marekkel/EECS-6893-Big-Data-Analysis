#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spark analytics for Ticketmaster events.

Reads the cleaned Parquet produced by spark_etl_events.py and generates:
  - Events per year & genre
  - Top cities by number of events
  - Top artists by upcoming events
  - Events per weekday

Usage example:

gcloud dataproc jobs submit pyspark spark_analysis_events.py \
  --cluster=<your-cluster> \
  --region=us-east1 \
  -- --input gs://<your-bucket>/ticketmaster_processed/events_parquet \
     --output gs://<your-bucket>/ticketmaster_analytics
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark(app_name: str = "TicketmasterAnalytics") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    return spark


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Input Parquet path, e.g. gs://bucket/ticketmaster_processed/events_parquet"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Output base path for analytics, e.g. gs://bucket/ticketmaster_analytics"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    spark = build_spark()

    print(f"[INFO] Reading cleaned events from: {args.input}")
    df = spark.read.parquet(args.input)

    print("[INFO] Schema of cleaned events:")
    df.printSchema()

    # Ensure Date_parsed exists
    if "Date_parsed" not in df.columns:
        df = df.withColumn("Date_parsed", F.to_date(F.col("Date"), "yyyy-MM-dd"))

    # Filter out rows without a parsed date
    df = df.filter(F.col("Date_parsed").isNotNull())

    # Add year and weekday columns for analysis
    df = (
        df
        .withColumn("year", F.year("Date_parsed"))
        .withColumn("weekday", F.date_format("Date_parsed", "E"))  # Mon, Tue, ...
    )

    # 1) Events per year & genre
    events_per_year_genre = (
        df.groupBy("year", "Genre")
          .agg(F.count("*").alias("event_count"))
          .orderBy("year", F.desc("event_count"))
    )

    events_per_year_genre_out = f"{args.output}/events_per_year_genre"
    print(f"[INFO] Writing events_per_year_genre to: {events_per_year_genre_out}")
    (
        events_per_year_genre
        .coalesce(1)  # small result, coalesce to one file for convenience
        .write.mode("overwrite").option("header", "true").csv(events_per_year_genre_out)
    )

    # 2) Top cities by number of events
    top_cities = (
        df.groupBy("Venue_City", "Venue_State")
          .agg(F.count("*").alias("event_count"))
          .orderBy(F.desc("event_count"))
          .limit(50)
    )

    top_cities_out = f"{args.output}/top_cities"
    print(f"[INFO] Writing top_cities to: {top_cities_out}")
    (
        top_cities
        .coalesce(1)
        .write.mode("overwrite").option("header", "true").csv(top_cities_out)
    )

    # 3) Top artists by upcoming events (sum and avg)
    top_artists = (
        df.groupBy("Artist")
          .agg(
              F.count("*").alias("event_count"),
              F.sum("Upcoming_Events_Artist").alias("sum_upcoming_artist"),
              F.avg("Upcoming_Events_Artist").alias("avg_upcoming_artist"),
          )
          .orderBy(F.desc("sum_upcoming_artist"))
          .limit(50)
    )

    top_artists_out = f"{args.output}/top_artists"
    print(f"[INFO] Writing top_artists to: {top_artists_out}")
    (
        top_artists
        .coalesce(1)
        .write.mode("overwrite").option("header", "true").csv(top_artists_out)
    )

    # 4) Events per weekday
    events_per_weekday = (
        df.groupBy("weekday")
          .agg(F.count("*").alias("event_count"))
          .orderBy("weekday")
    )

    events_per_weekday_out = f"{args.output}/events_per_weekday"
    print(f"[INFO] Writing events_per_weekday to: {events_per_weekday_out}")
    (
        events_per_weekday
        .coalesce(1)
        .write.mode("overwrite").option("header", "true").csv(events_per_weekday_out)
    )

    print("[INFO] Analytics done.")
    spark.stop()


if __name__ == "__main__":
    main()
