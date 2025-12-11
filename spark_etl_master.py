#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spark ETL for master_df.csv data.

将 master_df.csv 数据清洗、转换并输出为 Parquet 格式。

Usage:
本地运行:
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
    """将字符串列表转换为第一个元素"""
    return F.regexp_extract(F.col(col_name), r"\['([^']+)'\]", 1)


def main():
    args = parse_args()
    spark = build_spark()

    print(f"[INFO] Reading master_df.csv from: {args.input}")
    
    # 读取 CSV
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(args.input)
    
    print("[INFO] Original schema:")
    df.printSchema()
    print(f"[INFO] Total records: {df.count()}")

    # 数据清洗和转换
    df_clean = (
        df
        # 1. 解析日期列
        .withColumn("event_date", F.to_date(F.col("date")))
        .withColumn("presale_start", F.to_timestamp(F.col("presale_date_start")))
        .withColumn("presale_end", F.to_timestamp(F.col("presale_date_end")))
        .withColumn("sale_start", F.to_timestamp(F.col("TM_sale_date_start")))
        
        # 2. 解析列表字段（artists, spotify_followers, spotify_popularity）
        .withColumn("artist", parse_list_column("artists"))
        .withColumn("spotify_followers_clean", parse_list_column("spotify_followers"))
        .withColumn("spotify_popularity_clean", parse_list_column("spotify_popularity"))
        
        # 3. 类型转换 - 价格字段
        .withColumn("tm_max_price", F.col("TM_max").cast(DoubleType()))
        .withColumn("tm_min_price", F.col("TM_min").cast(DoubleType()))
        .withColumn("sg_avg_price", F.col("SG_average_price").cast(DoubleType()))
        .withColumn("sg_min_price", F.col("SG_min_price").cast(DoubleType()))
        .withColumn("sg_max_price", F.col("SG_max_price").cast(DoubleType()))
        .withColumn("sh_max_price", F.col("SH_max_price").cast(DoubleType()))
        .withColumn("sh_min_price", F.col("SH_min_price").cast(DoubleType()))
        
        # 4. 类型转换 - 计数字段
        .withColumn("sg_listing_count", F.col("SG_listing_count").cast(IntegerType()))
        .withColumn("sh_total_postings", F.col("SH_total_postings").cast(IntegerType()))
        .withColumn("sh_total_tickets", F.col("SH_total_tickets").cast(IntegerType()))
        
        # 5. 类型转换 - Spotify 字段
        .withColumn("spotify_followers", F.col("spotify_followers_clean").cast(IntegerType()))
        .withColumn("spotify_popularity", F.col("spotify_popularity_clean").cast(IntegerType()))
        
        # 6. 类型转换 - 地理坐标
        .withColumn("venue_lat", F.col("TM_venue _lat").cast(DoubleType()))
        .withColumn("venue_long", F.col("TM_venue_long").cast(DoubleType()))
        
        # 7. 计算特征
        .withColumn("price_range", F.col("tm_max_price") - F.col("tm_min_price"))
        .withColumn("has_spotify_data", F.col("spotify_followers").isNotNull())
        .withColumn("has_secondary_market", 
                    (F.col("sg_listing_count") > 0) | (F.col("sh_total_postings") > 0))
        
        # 8. 时间特征
        .withColumn("year", F.year("event_date"))
        .withColumn("month", F.month("event_date"))
        .withColumn("weekday", F.dayofweek("event_date"))
        .withColumn("is_weekend", F.col("weekday").isin([1, 7]))  # 1=Sunday, 7=Saturday
    )

    # 选择最终列
    df_final = df_clean.select(
        # 基本信息
        F.col("TM_id").alias("event_id"),
        F.col("event_title"),
        F.col("artist"),
        "event_date",
        
        # 场馆信息
        F.col("venue"),
        F.col("venue_city").alias("city"),
        F.col("venue_state").alias("state"),
        "venue_lat",
        "venue_long",
        
        # 分类信息
        F.col("genre"),
        F.col("subGenre").alias("subgenre"),
        F.col("event_type"),
        
        # Ticketmaster 价格
        "tm_max_price",
        "tm_min_price",
        "price_range",
        
        # SeatGeek 数据
        "sg_avg_price",
        "sg_min_price",
        "sg_max_price",
        "sg_listing_count",
        F.col("SG_artists_score").cast(DoubleType()).alias("sg_artist_score"),
        F.col("SG_venue_score").cast(DoubleType()).alias("sg_venue_score"),
        
        # StubHub 数据
        "sh_max_price",
        "sh_min_price",
        "sh_total_postings",
        "sh_total_tickets",
        
        # Spotify 数据
        "spotify_followers",
        "spotify_popularity",
        "has_spotify_data",
        
        # 市场特征
        "has_secondary_market",
        
        # 时间特征
        "year",
        "month",
        "weekday",
        "is_weekend",
        
        # 销售日期
        "presale_start",
        "presale_end",
        "sale_start",
        
        # 其他
        F.col("promoter"),
        F.col("span multiple days").alias("span_multiple_days")
    )

    # 数据质量检查
    print("\n[INFO] Data Quality Check:")
    print(f"  Total records: {df_final.count()}")
    print(f"  Records with valid event_date: {df_final.filter(F.col('event_date').isNotNull()).count()}")
    print(f"  Records with artist: {df_final.filter(F.col('artist').isNotNull()).count()}")
    print(f"  Records with Spotify data: {df_final.filter(F.col('has_spotify_data')).count()}")
    print(f"  Records with secondary market: {df_final.filter(F.col('has_secondary_market')).count()}")

    # 去重（基于 event_id）
    df_dedup = df_final.dropDuplicates(["event_id"])
    print(f"\n[INFO] After deduplication: {df_dedup.count()}")

    # 过滤必需字段非空
    df_filtered = df_dedup.filter(
        F.col("event_id").isNotNull() &
        F.col("event_date").isNotNull() &
        F.col("artist").isNotNull()
    )
    print(f"[INFO] After filtering (event_id, event_date, artist not null): {df_filtered.count()}")

    # 显示示例数据
    print("\n[INFO] Sample data:")
    df_filtered.show(5, truncate=False)

    # 写入 Parquet（按年月分区）
    print(f"\n[INFO] Writing Parquet to: {args.output}")
    (
        df_filtered
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(args.output)
    )

    # 统计信息
    print("\n[STATS] Final Statistics:")
    df_filtered.select(
        F.count("*").alias("total_records"),
        F.countDistinct("artist").alias("unique_artists"),
        F.countDistinct("city").alias("unique_cities"),
        F.avg("tm_min_price").alias("avg_tm_price"),
        F.avg("spotify_popularity").alias("avg_spotify_popularity"),
        F.sum(F.when(F.col("has_secondary_market"), 1).otherwise(0)).alias("secondary_market_count")
    ).show()

    print("[INFO] ETL complete.")
    spark.stop()


if __name__ == "__main__":
    main()
