#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spark ML model for Ticketmaster events.

Task:
  Predict Upcoming_Events_Artist (artist future popularity proxy)
  from event-level features (genre, city, state, venue popularity, etc.)

Usage example (Dataproc):

gcloud dataproc jobs submit pyspark spark_ml_events.py \
  --cluster=<your-cluster> \
  --region=us-east1 \
  -- --input gs://<your-bucket>/ticketmaster_processed/events_parquet \
     --metrics-output gs://<your-bucket>/ticketmaster_ml/metrics \
     --model-output gs://<your-bucket>/ticketmaster_ml/models/rf_upcoming_artist
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator


def build_spark(app_name: str = "TicketmasterML") -> SparkSession:
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
        help="Input Parquet path (cleaned events), e.g. gs://bucket/ticketmaster_processed/events_parquet"
    )
    parser.add_argument(
        "--metrics-output",
        type=str,
        required=True,
        help="Output path for metrics (JSON/CSV), e.g. gs://bucket/ticketmaster_ml/metrics"
    )
    parser.add_argument(
        "--model-output",
        type=str,
        required=True,
        help="Output path to save trained model, e.g. gs://bucket/ticketmaster_ml/models/rf_upcoming_artist"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    spark = build_spark()

    print(f"[INFO] Reading cleaned events from: {args.input}")
    df = spark.read.parquet(args.input)

    print("[INFO] Schema of cleaned events:")
    df.printSchema()

    # 确保 Date_parsed 存在
    if "Date_parsed" not in df.columns:
        df = df.withColumn("Date_parsed", F.to_date(F.col("Date"), "yyyy-MM-dd"))

    # 只保留有 label 的样本：Upcoming_Events_Artist
    df_ml = (
        df
        .filter(F.col("Upcoming_Events_Artist").isNotNull())
        .withColumn("label", F.col("Upcoming_Events_Artist").cast("double"))
        .filter(F.col("label") >= 0)  # 去掉奇怪负值（如果有）
    )

    # 一些简单的时间特征
    df_ml = (
        df_ml
        .withColumn("year", F.year("Date_parsed"))
        .withColumn("month", F.month("Date_parsed"))
        # Spark 3.x 推荐用 dayofweek：1=Sunday, 7=Saturday
        .withColumn("weekday", F.dayofweek("Date_parsed"))  
    )


    # 选定特征列
    categorical_cols = [
        "Segment",
        "Genre",
        "SubGenre",
        "Venue_City",
        "Venue_State",
        "Venue_Country",
        # "Artist",   # 如果维度太高可以先不加
    ]

    numeric_cols = [
        "Upcoming_Events_Venue",
        "year",
        "month",
        "weekday",
    ]

    # 先简单填充缺失的 numeric 为 0
    for c in numeric_cols:
        df_ml = df_ml.withColumn(
            c,
            F.when(F.col(c).isNull(), F.lit(0)).otherwise(F.col(c))
        )

    # 对每一个类别特征做 StringIndexer + OneHotEncoder
    indexers = []
    encoders = []
    for c in categorical_cols:
        indexer = StringIndexer(
            inputCol=c,
            outputCol=f"{c}_idx",
            handleInvalid="keep"   # 未见过的类别也放在一个 bucket 里
        )
        encoder = OneHotEncoder(
            inputCols=[f"{c}_idx"],
            outputCols=[f"{c}_ohe"]
        )
        indexers.append(indexer)
        encoders.append(encoder)

    # 特征向量拼接
    feature_cols = [f"{c}_ohe" for c in categorical_cols] + numeric_cols

    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )

    # 回归模型：随机森林
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",
        numTrees=80,
        maxDepth=10,
        seed=42
    )

    # 组装 Pipeline
    stages = []
    stages.extend(indexers)
    stages.extend(encoders)
    stages.append(assembler)
    stages.append(rf)

    pipeline = Pipeline(stages=stages)

    # 划分 train / test
    train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)

    print(f"[INFO] Training samples: {train_df.count()}, Test samples: {test_df.count()}")

    # 训练
    print("[INFO] Training RandomForestRegressor model ...")
    model = pipeline.fit(train_df)

    # 预测
    print("[INFO] Evaluating on test set ...")
    pred_df = model.transform(test_df)

    evaluator_rmse = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="rmse"
    )
    evaluator_mae = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="mae"
    )
    evaluator_r2 = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="r2"
    )

    rmse = evaluator_rmse.evaluate(pred_df)
    mae = evaluator_mae.evaluate(pred_df)
    r2 = evaluator_r2.evaluate(pred_df)

    print(f"[METRIC] RMSE = {rmse}")
    print(f"[METRIC] MAE  = {mae}")
    print(f"[METRIC] R2   = {r2}")

    # 把 metrics 写到 GCS / 本地
    metrics_out = args.metrics_output.rstrip("/")
    metrics_df = spark.createDataFrame(
        [(float(rmse), float(mae), float(r2))],
        ["rmse", "mae", "r2"]
    )

    print(f"[INFO] Writing metrics to: {metrics_out}")
    (
        metrics_df
        .coalesce(1)
        .write.mode("overwrite")
        .json(metrics_out)
    )

    # 保存模型
    model_out = args.model_output.rstrip("/")
    print(f"[INFO] Saving model to: {model_out}")
    model.write().overwrite().save(model_out)

    print("[INFO] ML training & evaluation done.")
    spark.stop()


if __name__ == "__main__":
    main()
