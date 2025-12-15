#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spark ML for master_df.csv - 票价预测模型

使用机器学习预测活动的一手市场价格（Min）

特征包括:
  - 艺术家 Spotify 热度
  - 音乐类型和子类型
  - 城市和州
  - 时间特征（年、月、星期几）

目标变量: tm_max_price

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

    # 数据准备
    print("[INFO] Preparing data for ML...")
    df_ml = (
        df.filter(
            F.col("tm_min_price").isNotNull() &
            (F.col("tm_min_price") >= 0) &
            F.col("artist").isNotNull()
        )
        # 填充缺失值
        .fillna({
            "spotify_popularity": 0,
            "genre": "Unknown",
            "subgenre": "Unknown",
            "city": "Unknown",
            "state": "Unknown"
        })
    )
    print(f"[INFO] ML dataset size: {df_ml.count()}")
    
        
    # 分割数据集
    print("[INFO] Splitting data: 80% train, 20% test...")
    train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)
    print(f"  Train set: {train_df.count()}")
    print(f"  Test set: {test_df.count()}")

    global_avg = train_df.select(F.avg("tm_min_price")).first()[0]  

    def add_avg_encoding(df, ref_df, col):
        avg_df = (
            ref_df
            .groupBy(col)
            .agg(F.avg("tm_min_price").alias(f"{col}_avg_price"))
        )
        return df.join(avg_df, on=col, how="left") \
                .fillna({f"{col}_avg_price": global_avg})        
                
    # 只用train data取平均
    for c in ["state", "city", "genre", "subgenre"]:
        train_df = add_avg_encoding(train_df, train_df, c)
        test_df  = add_avg_encoding(test_df,  train_df, c)
        
    # 补上null
    avg_cols = ["state_avg_price", "city_avg_price", "genre_avg_price", "subgenre_avg_price"]
    train_df = train_df.fillna(0, subset=avg_cols)
    test_df  = test_df.fillna(0, subset=avg_cols)

    # 保存avg结果
    save_base = f"{args.output}/avg_encoding_min"
    
    def save_avg_map(ref_df, col):
        (
            ref_df
            .groupBy(col)
            .agg(F.avg("tm_min_price").alias("avg_price"))
            .coalesce(1) 
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(f"{save_base}/{col}_avg")
        )

    for c in ["state", "city", "genre", "subgenre"]:
        save_avg_map(train_df, c)
    
    # 保存 global 平均值
    global_avg_df = spark.createDataFrame(
        [("global", float(global_avg))],
        ["key", "avg_price"]
    )

    (
        global_avg_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(f"{save_base}/global_avg")
    )

    # 特征工程
    print("[INFO] Feature engineering...")
    
    # 数值特征
    numeric_features = ["spotify_popularity", "year", "month", "weekday", 
                        "state_avg_price", "city_avg_price", "genre_avg_price", "subgenre_avg_price"]

    # 组合所有特征
    assembler = VectorAssembler(
        inputCols=numeric_features,
        outputCol="features",
        handleInvalid="error"
    )
    
    # 选择模型
    print(f"[INFO] Model type: {args.model_type}")
    if args.model_type == "rf":
        model = RandomForestRegressor(
            featuresCol="features",
            labelCol="tm_min_price",
            numTrees=200,
            maxDepth=20,
            seed=42
        )
    elif args.model_type == "gbt":
        model = GBTRegressor(
            featuresCol="features",
            labelCol="tm_min_price",
            maxIter=100,
            maxDepth=6,
            seed=42
        )
    else:  # lr
        model = LinearRegression(
            featuresCol="features",
            labelCol="tm_min_price",
            regParam=0.1,
            elasticNetParam=0.3,  # 0.5 = L1 和 L2 混合
            maxIter=200
        )

    pipeline = Pipeline(stages=[assembler, model])



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
        labelCol="tm_min_price",
        predictionCol="prediction",
        metricName="rmse"
    )
    evaluator_mae = RegressionEvaluator(
        labelCol="tm_min_price",
        predictionCol="prediction",
        metricName="mae"
    )
    evaluator_r2 = RegressionEvaluator(
        labelCol="tm_min_price",
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
        F.col("tm_min_price").alias("actual_price"),
        F.col("prediction").alias("predicted_price"),
        F.abs(F.col("tm_min_price") - F.col("prediction")).alias("error")
    ).show(20, truncate=False)

    # 保存预测结果
    predictions_output = f"{args.output}/predictions"
    print(f"\n[INFO] Saving predictions to: {predictions_output}")
    predictions.select(
        "event_id",
        "artist",
        "genre",
        "event_date",
        "tm_min_price",
        "prediction",
        "spotify_popularity"
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
        print("\n[INFO] Extracting feature importance...")

        feature_importance = list(pipeline_model.stages[-1].featureImportances)

        # 数值特征名（顺序与 VectorAssembler inputCols 一致）
        feature_names = numeric_features

        # 安全检查
        if len(feature_importance) != len(feature_names):
            print(
                f"❗ WARNING: feature importance length = {len(feature_importance)}, "
                f"but feature names length = {len(feature_names)}"
            )

        importance_list = list(zip(feature_names, feature_importance))
        importance_list.sort(key=lambda x: x[1], reverse=True)

        print("\n特征重要性排序:")
        for feat, imp in importance_list:
            print(f"  {feat}: {imp:.5f}")

        # 保存为 CSV
        importance_output = f"{args.output}/feature_importance"
        importance_df = spark.createDataFrame(
            [(f, float(v)) for f, v in importance_list],
            ["feature", "importance"]
        )

        importance_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(importance_output)
        print(f"[INFO] Feature importance saved to: {importance_output}")
        

    print("\n[INFO] ML pipeline complete!")
    spark.stop()


if __name__ == "__main__":
    main()
