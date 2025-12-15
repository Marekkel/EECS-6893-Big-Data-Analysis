#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spark analytics for master_df.csv events data.

对清洗后的数据进行多维度分析：
  1. 年份 × 音乐类型分析
  2. 热门城市排名
  3. 热门艺术家排名（基于 Spotify 数据）
  4. 星期几分布
  5. 二级市场分析
  6. 价格分析

Usage:
本地运行:
    spark-submit spark_analysis_master.py \
      --input output/master_parquet \
      --output output/analytics

Dataproc:
    gcloud dataproc jobs submit pyspark spark_analysis_master.py \
      --cluster=<cluster-name> \
      --region=us-east1 \
      -- --input gs://bucket/output/master_parquet \
         --output gs://bucket/output/analytics
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark(app_name: str = "MasterDataAnalytics") -> SparkSession:
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
        help="Input Parquet path from ETL step"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Output base path for analytics results"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    spark = build_spark()

    print(f"[INFO] Reading processed data from: {args.input}")
    df = spark.read.parquet(args.input)

    print("[INFO] Schema:")
    df.printSchema()
    print(f"[INFO] Total records: {df.count()}")

    # ============================================================
    # 1. 年份 × 音乐类型分析
    # ============================================================
    print("\n[1/6] Analyzing events per year & genre...")
    events_per_year_genre = (
        df.groupBy("year", "genre")
          .agg(
              F.count("*").alias("event_count"),
              F.avg("tm_min_price").alias("avg_price"),
              F.avg("spotify_popularity").alias("avg_popularity")
          )
          .orderBy("year", F.desc("event_count"))
    )

    output_1 = f"{args.output}/events_per_year_genre"
    print(f"[INFO] Writing to: {output_1}")
    events_per_year_genre.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_1)

    # ============================================================
    # 2. 热门城市排名
    # ============================================================
    print("\n[2/6] Analyzing top cities...")
    top_cities = (
        df.groupBy("city", "state")
          .agg(
              F.count("*").alias("event_count"),
              F.countDistinct("artist").alias("unique_artists"),
              F.avg("tm_min_price").alias("avg_price"),
              F.sum(F.when(F.col("has_secondary_market"), 1).otherwise(0)).alias("secondary_market_events")
          )
          .orderBy(F.desc("event_count"))
          .limit(50)
    )

    output_2 = f"{args.output}/top_cities"
    print(f"[INFO] Writing to: {output_2}")
    top_cities.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_2)

    # ============================================================
    # 3. 热门艺术家排名（基于 Spotify 数据）
    # ============================================================
    print("\n[3/6] Analyzing top artists...")
    top_artists = (
        df.filter(F.col("spotify_popularity").isNotNull())
          .groupBy("artist")
          .agg(
              F.count("*").alias("event_count"),
              F.avg("spotify_popularity").alias("avg_spotify_popularity"),
              F.first("spotify_followers").alias("spotify_followers"),
              F.avg("tm_min_price").alias("avg_ticket_price"),
              F.countDistinct("city").alias("cities_performed")
          )
          .orderBy(F.desc("avg_spotify_popularity"))
          .limit(100)
    )

    output_3 = f"{args.output}/top_artists"
    print(f"[INFO] Writing to: {output_3}")
    top_artists.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_3)

    # ============================================================
    # 4. 星期几分布
    # ============================================================
    print("\n[4/6] Analyzing events per weekday...")
    
    # 定义星期几映射
    weekday_map = F.when(F.col("weekday") == 1, "Sunday") \
                   .when(F.col("weekday") == 2, "Monday") \
                   .when(F.col("weekday") == 3, "Tuesday") \
                   .when(F.col("weekday") == 4, "Wednesday") \
                   .when(F.col("weekday") == 5, "Thursday") \
                   .when(F.col("weekday") == 6, "Friday") \
                   .when(F.col("weekday") == 7, "Saturday")
    
    events_per_weekday = (
        df.withColumn("weekday_name", weekday_map)
          .groupBy("weekday", "weekday_name", "is_weekend")
          .agg(
              F.count("*").alias("event_count"),
              F.avg("tm_min_price").alias("avg_price")
          )
          .orderBy("weekday")
    )

    output_4 = f"{args.output}/events_per_weekday"
    print(f"[INFO] Writing to: {output_4}")
    events_per_weekday.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_4)

    # ============================================================
    # 5. 二级市场分析
    # ============================================================
    print("\n[5/6] Analyzing secondary market...")
    
    secondary_market_analysis = (
        df.filter(F.col("has_secondary_market"))
          .groupBy("genre")
          .agg(
              F.count("*").alias("event_count"),
              F.avg("sg_avg_price").alias("avg_seatgeek_price"),
              F.avg("sh_max_price").alias("avg_stubhub_max"),
              F.avg("tm_min_price").alias("avg_tm_price"),
              # 计算溢价率
              F.avg(
                  (F.col("sg_avg_price") - F.col("tm_min_price")) / F.col("tm_min_price") * 100
              ).alias("avg_premium_pct")
          )
          .orderBy(F.desc("event_count"))
    )

    output_5 = f"{args.output}/secondary_market_by_genre"
    print(f"[INFO] Writing to: {output_5}")
    secondary_market_analysis.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_5)

    # ============================================================
    # 6. 价格分析
    # ============================================================
    print("\n[6/6] Analyzing price distribution...")
    
    price_analysis = (
        df.filter(F.col("tm_min_price").isNotNull())
          .groupBy("state")
          .agg(
              F.count("*").alias("event_count"),
              F.min("tm_min_price").alias("min_price"),
              F.avg("tm_min_price").alias("avg_price"),
              F.max("tm_max_price").alias("max_price"),
              F.avg("price_range").alias("avg_price_range")
          )
          .orderBy(F.desc("avg_price"))
          .limit(50)
    )

    output_6 = f"{args.output}/price_by_state"
    print(f"[INFO] Writing to: {output_6}")
    price_analysis.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_6)

    # ============================================================
    # 汇总报告
    # ============================================================
    print("\n" + "="*80)
    print("ANALYTICS SUMMARY")
    print("="*80)
    
    print("\n总体统计:")
    df.select(
        F.count("*").alias("total_events"),
        F.countDistinct("artist").alias("unique_artists"),
        F.countDistinct("city").alias("unique_cities"),
        F.countDistinct("state").alias("unique_states"),
        F.min("event_date").alias("earliest_event"),
        F.max("event_date").alias("latest_event")
    ).show(truncate=False)
    
    print("\n音乐类型分布 (Top 10):")
    df.groupBy("genre").count().orderBy(F.desc("count")).limit(10).show(truncate=False)
    
    print("\nSpotify 数据覆盖率:")
    total = df.count()
    with_spotify = df.filter(F.col("has_spotify_data")).count()
    print(f"  有 Spotify 数据: {with_spotify} / {total} ({with_spotify/total*100:.1f}%)")
    
    print("\n二级市场覆盖率:")
    with_secondary = df.filter(F.col("has_secondary_market")).count()
    print(f"  有二级市场数据: {with_secondary} / {total} ({with_secondary/total*100:.1f}%)")

    print("\n[INFO] Analytics complete. All results saved to:", args.output)
    spark.stop()


if __name__ == "__main__":
    main()
