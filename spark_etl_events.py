#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
PySpark ETL for Ticketmaster events (no priceRanges).

- 读取 raw JSONL(每行一个 event JSON)
- 扁平化出常用字段（和你原来的 pandas DataFrame 基本对齐）
- 不依赖 priceRanges 字段（当前数据中没有）
- 类型转换（日期、数值）
- 过滤缺失严重的行
- 写出为 Parquet
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark(app_name: str = "TicketmasterETL") -> SparkSession:
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
        help="Input path pattern for raw JSONL, e.g. gs://bucket/ticketmaster_raw/dt=*/events_*.jsonl"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Output path for cleaned Parquet, e.g. gs://bucket/ticketmaster_processed/events_parquet"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    spark = build_spark()

    print(f"[INFO] Reading raw data from: {args.input}")
    df_raw = spark.read.json(args.input)

    print("[INFO] Raw schema:")
    df_raw.printSchema()

    # 取第一个 venue / attraction / classification
    venue_col = F.col("_embedded.venues").getItem(0)
    artist_col = F.col("_embedded.attractions").getItem(0)
    classification_col = F.col("classifications").getItem(0)

    # 扁平化选择字段（不再依赖 priceRanges）
    df_flat = (
        df_raw
        .withColumn("venue", venue_col)
        .withColumn("artist", artist_col)
        .withColumn("cls", classification_col)
        .select(
            # Basic info
            F.col("id").alias("ID"),
            F.col("name").alias("Name"),
            F.col("dates.start.localDate").alias("Date"),

            # Artist & classification
            F.col("artist.name").alias("Artist"),
            F.col("cls.segment.name").alias("Segment"),
            F.col("cls.genre.name").alias("Genre"),
            F.col("cls.subGenre.name").alias("SubGenre"),

            # Promoter
            F.col("promoter.name").alias("Promoter"),

            # Price ranges: 当前数据中没有 priceRanges，用 NULL 占位（方便以后扩展）
            F.lit(None).cast("string").alias("Price_Ranges_Type"),
            F.lit(None).cast("string").alias("Price_Ranges_Currency"),
            F.lit(None).cast("double").alias("Price_Ranges_Max"),
            F.lit(None).cast("double").alias("Price_Ranges_Min"),

            # Venue info
            F.col("venue.name").alias("Venue"),
            F.col("venue.city.name").alias("Venue_City"),
            F.col("venue.state.stateCode").alias("Venue_State"),
            F.col("venue.country.name").alias("Venue_Country"),
            F.col("venue.timezone").alias("Venue_Timezone"),

            # Upcoming events counts
            F.col("venue.upcomingEvents._total").alias("Upcoming_Events_Venue"),
            F.col("artist.upcomingEvents._total").alias("Upcoming_Events_Artist"),
        )
    )

    print("[INFO] Flattened schema (before type casting):")
    df_flat.printSchema()

    # 类型转换：日期、数值
    df_typed = (
        df_flat
        .withColumn("Date_parsed", F.to_date(F.col("Date"), "yyyy-MM-dd"))
        .withColumn(
            "Upcoming_Events_Venue",
            F.col("Upcoming_Events_Venue").cast("int")
        )
        .withColumn(
            "Upcoming_Events_Artist",
            F.col("Upcoming_Events_Artist").cast("int")
        )
    )
    # Price_Ranges_* 已经是 double/ string NULL，占位即可，无需再 cast

    # 去重（按 ID）
    df_dedup = df_typed.dropDuplicates(["ID"])

    # essential_cols：先只要求这些一定非空（不要再要求价格列，否则全部被过滤掉）
    essential_cols = [
        "Date",
        "Artist",
        "Segment",
        "Genre",
        "Venue_Country",
        "Venue_State",
        "Upcoming_Events_Artist",
    ]

    cond = None
    for c in essential_cols:
        this_cond = F.col(c).isNotNull()
        cond = this_cond if cond is None else (cond & this_cond)

    df_clean = df_dedup.filter(cond)

    print("[INFO] Example rows after cleaning:")
    df_clean.show(5, truncate=False)

    print(f"[INFO] Writing cleaned Parquet to: {args.output}")
    (
        df_clean
        # 如需按日期分区，可改成 partitionBy("Date")
        .write.mode("overwrite").parquet(args.output)
    )

    print("[INFO] ETL done.")
    spark.stop()


if __name__ == "__main__":
    main()
