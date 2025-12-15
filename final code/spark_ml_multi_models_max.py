#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spark 多模型训练 - master_df.csv 数据

训练 6 种回归模型预测二级市场价格（SeatGeek 平均价格）:
  1. Linear Regression (普通线性回归)
  2. Lasso Regression (L1 正则化)
  3. Elastic Net (L1+L2 正则化)
  4. Decision Tree (决策树)
  5. Random Forest (随机森林)
  6. Gradient Boosting Tree (梯度提升树)

Usage:
本地运行:
    spark-submit spark_ml_multi_models.py \
      --input output/master_parquet \
      --output output/ml_multi_models

Dataproc:
    gcloud dataproc jobs submit pyspark spark_ml_multi_models.py \
      --cluster=<cluster-name> \
      --region=us-east1 \
      -- --input gs://bucket/output/master_parquet \
         --output gs://bucket/output/ml_multi_models
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator


def build_spark(app_name: str = "MultiModelML") -> SparkSession:
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
        help="Output base path for models and metrics"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    spark = build_spark()

    print(f"[INFO] Reading processed data from: {args.input}")
    df = spark.read.parquet(args.input)
    print("[INFO] Schema:")
    df.printSchema()
    
    # 数据准备
    print("[INFO] Preparing data for ML...")
    df_ml = (
        df.filter(
            F.col("tm_max_price").isNotNull() &
            (F.col("tm_max_price") > 0) &
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

    # # 用平均价格代替类别标签，避免类别标签维度过高影响大
    # def add_avg_encoding(df, ref_df, col):
    #     avg_df = (
    #         ref_df
    #         .groupBy(col)
    #         .agg(F.avg("tm_max_price").alias(f"{col}_avg_price"))
    #     )
    #     return df.join(avg_df, on=col, how="left")

    global_avg = train_df.select(F.avg("tm_max_price")).first()[0]  

    def add_avg_encoding(df, ref_df, col):
        avg_df = (
            ref_df
            .groupBy(col)
            .agg(F.avg("tm_max_price").alias(f"{col}_avg_price"))
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
    
    # 构建并应用特征工程 Pipeline
    print("[INFO] Applying feature engineering...")
    pipeline = Pipeline(stages=[assembler])
    feature_model = pipeline.fit(train_df)
    train_data = feature_model.transform(train_df)
    test_data = feature_model.transform(test_df)

    # 缓存数据以加速多次训练
    train_data = train_data.cache()
    test_data = test_data.cache()

    print(f"[INFO] Feature vector size: {train_data.select('features').head()[0].size}")

    # ============================================================
    # 定义 6 种回归模型
    # ============================================================
    print("\n" + "="*80)
    print("定义多个回归模型")
    print("="*80)

    # 1. 普通线性回归
    lr = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        maxIter=100
    )

    # 2. Lasso 回归（L1 正则化）
    lasso = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.1,
        elasticNetParam=1.0,  # 1.0 = 纯 L1
        maxIter=100
    )

    # 3. Elastic Net（L1 + L2 正则化）
    elastic_net = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.1,
        elasticNetParam=0.7,  # 0.5 = L1 和 L2 混合
        maxIter=100
    )

    # 4. 决策树
    dt = DecisionTreeRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        maxDepth=8,
        seed=42
    )

    # 5. 随机森林
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        numTrees=100,
        maxDepth=20,
        seed=42
    )

    # 6. 梯度提升树
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        maxIter=50,
        maxDepth=2,
        seed=42
    )

    regression_models = {
        "linear_regression": lr,
        "lasso_regression": lasso,
        "elastic_net": elastic_net,
        "decision_tree": dt,
        "random_forest": rf,
        "gbt": gbt
    }

    print(f"[INFO] 将训练 {len(regression_models)} 个回归模型")

    # ============================================================
    # 训练和评估所有模型
    # ============================================================
    evaluator_rmse = RegressionEvaluator(metricName="rmse", labelCol="tm_max_price", predictionCol="prediction")
    evaluator_mae = RegressionEvaluator(metricName="mae", labelCol="tm_max_price", predictionCol="prediction")
    evaluator_r2 = RegressionEvaluator(metricName="r2", labelCol="tm_max_price", predictionCol="prediction")

    metrics_list = []
    
    for model_name, model in regression_models.items():
        print("\n" + "="*80)
        print(f"训练模型: {model_name}")
        print("="*80)

        # 训练模型
        trained_model = model.fit(train_data)
        
        # 预测
        pred_df = trained_model.transform(test_data)

        # 评估
        rmse = evaluator_rmse.evaluate(pred_df)
        mae = evaluator_mae.evaluate(pred_df)
        r2 = evaluator_r2.evaluate(pred_df)

        print(f"[METRICS]")
        print(f"  RMSE (Root Mean Squared Error): ${rmse:.2f}")
        print(f"  MAE  (Mean Absolute Error):     ${mae:.2f}")
        print(f"  R²   (R-squared):                {r2:.4f}")

        # 保存模型
        model_path = f"{args.output}/models/{model_name}"
        print(f"[INFO] 保存模型到: {model_path}")
        trained_model.write().overwrite().save(model_path)

        # 保存预测样例
        predictions_sample = pred_df.select(
            "event_id",
            "artist",
            "genre",
            F.col("tm_max_price").alias("actual_price"),
            F.col("prediction").alias("predicted_price"),
            F.abs(F.col("tm_max_price") - F.col("prediction")).alias("error")
        ).limit(100)
        
        sample_output = f"{args.output}/predictions_sample/{model_name}"
        print(f"[INFO] 保存预测样例到: {sample_output}")
        predictions_sample.coalesce(1).write.mode("overwrite").option("header", "true").csv(sample_output)

        # 记录指标
        metrics_list.append((model_name, float(rmse), float(mae), float(r2)))

    # ============================================================
    # 保存所有模型的对比指标
    # ============================================================
    metrics_output = f"{args.output}/metrics_comparison"
    print(f"\n[INFO] 保存所有模型对比指标到: {metrics_output}")
    
    metrics_schema = ["model", "rmse", "mae", "r2"]
    metrics_df = spark.createDataFrame(metrics_list, metrics_schema)
    
    # 保存为 CSV
    metrics_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(metrics_output + "_csv")
    
    # 保存为 JSON
    metrics_df.coalesce(1).write.mode("overwrite").json(metrics_output + "_json")

    # 显示对比结果
    print("\n" + "="*80)
    print("所有模型性能对比")
    print("="*80)
    metrics_df.orderBy("rmse").show(truncate=False)

    # 找出最佳模型
    best_model = metrics_df.orderBy("rmse").first()
    print(f"\n🏆 最佳模型: {best_model['model']}")
    print(f"   RMSE: ${best_model['rmse']:.2f}")
    print(f"   MAE:  ${best_model['mae']:.2f}")
    print(f"   R²:   {best_model['r2']:.4f}")

    print("\n[INFO] 多模型训练完成！")
    print("\n📁 输出位置:")
    print(f"  - 模型文件:     {args.output}/models/")
    print(f"  - 预测样例:     {args.output}/predictions_sample/")
    print(f"  - 指标对比:     {args.output}/metrics_comparison_csv/")
    
    spark.stop()


if __name__ == "__main__":
    main()
