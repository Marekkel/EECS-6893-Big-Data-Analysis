#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spark ML for master_df.csv - 票价预测模型

使用机器学习预测活动的平均二级市场价格（SeatGeek 平均价格）

特征包括:
  - 艺术家 Spotify 热度和粉丝数
  - 音乐类型和子类型
  - 城市和州
  - 时间特征（年、月、星期几）
  - Ticketmaster 原始价格

目标变量: sg_avg_price (SeatGeek 平均价格)

Usage:
本地运行:
    spark-submit spark_ml_master.py \
      --input output/master_parquet \
      --output output/ml_results

Dataproc:
    gcloud dataproc jobs submit pyspark spark_ml_master.py \
      --cluster=<cluster-name> \
      --region=us-east1 \
      -- --input gs://bucket/output/master_parquet \
         --output gs://bucket/output/ml_results
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder, StandardScaler
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor, LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline


def build_spark(app_name: str = "MasterDataML") -> SparkSession:
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
        help="Input Parquet path from ETL step"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Output path for ML results"
    )
    parser.add_argument(
        "--model-type",
        type=str,
        default="rf",
        choices=["rf", "gbt", "lr"],
        help="Model type: rf (RandomForest), gbt (GradientBoosting), lr (LinearRegression)"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    spark = build_spark()

    print(f"[INFO] Reading processed data from: {args.input}")
    df = spark.read.parquet(args.input)

    # 数据准备：过滤有二级市场价格的记录
    print("[INFO] Preparing data for ML...")
    df_ml = (
        df.filter(
            F.col("sg_avg_price").isNotNull() &
            (F.col("sg_avg_price") > 0) &
            F.col("tm_min_price").isNotNull() &
            F.col("artist").isNotNull() &
            F.col("genre").isNotNull()
        )
        # 填充缺失值
        .fillna({
            "spotify_popularity": 0,
            "spotify_followers": 0,
            "sg_listing_count": 0,
            "subgenre": "Unknown",
            "city": "Unknown",
            "state": "Unknown"
        })
    )

    print(f"[INFO] ML dataset size: {df_ml.count()}")
    
    if df_ml.count() < 100:
        print("[ERROR] Not enough data for ML (< 100 records with sg_avg_price)")
        spark.stop()
        return

    # 特征工程
    print("[INFO] Feature engineering...")
    
    # 分类特征编码
    categorical_cols = ["genre", "subgenre", "state"]
    indexers = [
        StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep")
        for col in categorical_cols
    ]
    encoders = [
        OneHotEncoder(inputCol=f"{col}_idx", outputCol=f"{col}_vec")
        for col in categorical_cols
    ]

    # 数值特征
    numeric_features = [
        "tm_min_price",
        "tm_max_price",
        "price_range",
        "spotify_popularity",
        "spotify_followers",
        "sg_listing_count",
        "year",
        "month",
        "weekday"
    ]

    # 组合所有特征
    feature_cols = numeric_features + [f"{col}_vec" for col in categorical_cols]
    
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features_raw",
        handleInvalid="skip"
    )
    
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=False
    )

    # 选择模型
    print(f"[INFO] Model type: {args.model_type}")
    if args.model_type == "rf":
        model = RandomForestRegressor(
            featuresCol="features",
            labelCol="sg_avg_price",
            numTrees=100,
            maxDepth=10,
            seed=42
        )
    elif args.model_type == "gbt":
        model = GBTRegressor(
            featuresCol="features",
            labelCol="sg_avg_price",
            maxIter=100,
            maxDepth=5,
            seed=42
        )
    else:  # lr
        model = LinearRegression(
            featuresCol="features",
            labelCol="sg_avg_price",
            maxIter=100,
            regParam=0.1,
            elasticNetParam=0.5
        )

    # 构建 Pipeline
    pipeline = Pipeline(stages=indexers + encoders + [assembler, scaler, model])

    # 分割数据集
    print("[INFO] Splitting data: 80% train, 20% test...")
    train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)
    print(f"  Train set: {train_df.count()}")
    print(f"  Test set: {test_df.count()}")

    # 训练模型
    print("[INFO] Training model...")
    pipeline_model = pipeline.fit(train_df)

    # 预测
    print("[INFO] Making predictions...")
    predictions = pipeline_model.transform(test_df)

    # 评估
    print("\n" + "="*80)
    print("MODEL EVALUATION")
    print("="*80)

    evaluator_rmse = RegressionEvaluator(
        labelCol="sg_avg_price",
        predictionCol="prediction",
        metricName="rmse"
    )
    evaluator_mae = RegressionEvaluator(
        labelCol="sg_avg_price",
        predictionCol="prediction",
        metricName="mae"
    )
    evaluator_r2 = RegressionEvaluator(
        labelCol="sg_avg_price",
        predictionCol="prediction",
        metricName="r2"
    )

    rmse = evaluator_rmse.evaluate(predictions)
    mae = evaluator_mae.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)

    print(f"\nRMSE (Root Mean Squared Error): ${rmse:.2f}")
    print(f"MAE (Mean Absolute Error): ${mae:.2f}")
    print(f"R² (R-squared): {r2:.4f}")

    # 显示预测示例
    print("\n预测示例 (前 20 条):")
    predictions.select(
        "artist",
        "genre",
        "city",
        "tm_min_price",
        F.col("sg_avg_price").alias("actual_price"),
        F.col("prediction").alias("predicted_price"),
        F.abs(F.col("sg_avg_price") - F.col("prediction")).alias("error")
    ).show(20, truncate=False)

    # 保存预测结果
    predictions_output = f"{args.output}/predictions"
    print(f"\n[INFO] Saving predictions to: {predictions_output}")
    predictions.select(
        "event_id",
        "artist",
        "genre",
        "city",
        "event_date",
        "tm_min_price",
        "sg_avg_price",
        "prediction",
        "spotify_popularity",
        "spotify_followers"
    ).coalesce(5).write.mode("overwrite").option("header", "true").csv(predictions_output)

    # 保存评估指标
    metrics_output = f"{args.output}/metrics"
    print(f"[INFO] Saving metrics to: {metrics_output}")
    
    # 确保所有值都是 float 类型
    metrics_data = [
        ("RMSE", float(rmse)),
        ("MAE", float(mae)),
        ("R2", float(r2)),
        ("train_size", float(train_df.count())),
        ("test_size", float(test_df.count()))
    ]
    metrics_df = spark.createDataFrame(metrics_data, ["metric", "value"])
    metrics_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(metrics_output)

    # 特征重要性（仅 RandomForest 和 GBT）
    if args.model_type in ["rf", "gbt"]:
        print("\n特征重要性:")
        feature_importance = pipeline_model.stages[-1].featureImportances
        
        importance_list = []
        for idx, importance in enumerate(feature_importance):
            if idx < len(feature_cols):
                importance_list.append((feature_cols[idx], float(importance)))
        
        importance_list.sort(key=lambda x: x[1], reverse=True)
        
        print("\nTop 15 最重要特征:")
        for feat, imp in importance_list[:15]:
            print(f"  {feat}: {imp:.4f}")
        
        # 保存特征重要性
        importance_output = f"{args.output}/feature_importance"
        importance_df = spark.createDataFrame(importance_list, ["feature", "importance"])
        importance_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(importance_output)

    print("\n[INFO] ML pipeline complete!")
    spark.stop()


if __name__ == "__main__":
    main()
