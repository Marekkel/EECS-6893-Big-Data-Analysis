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

    # 数据准备：只保留有二级市场价格的记录
    print("[INFO] Preparing ML dataset...")
    df_ml = (
        df
        .filter(
            F.col("sg_avg_price").isNotNull() &
            (F.col("sg_avg_price") > 0) &
            F.col("tm_min_price").isNotNull() &
            F.col("artist").isNotNull() &
            F.col("genre").isNotNull()
        )
        # 目标变量：SeatGeek 平均价格
        .withColumn("label", F.col("sg_avg_price").cast("double"))
        .filter(F.col("label") >= 0)
    )

    print(f"[INFO] ML dataset size: {df_ml.count()}")
    
    if df_ml.count() < 100:
        print("[ERROR] Not enough data for ML (< 100 records with sg_avg_price)")
        spark.stop()
        return

    # 定义特征
    # 类别特征（需要编码）
    categorical_cols = ["genre", "subgenre", "state"]
    
    # 数值特征
    numeric_cols = [
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

    # 填充缺失值
    print("[INFO] Filling missing values...")
    for col in numeric_cols:
        df_ml = df_ml.withColumn(
            col,
            F.when(F.col(col).isNull(), F.lit(0)).otherwise(F.col(col))
        )
    
    for col in categorical_cols:
        df_ml = df_ml.fillna({col: "Unknown"})

    # 构建特征工程 Pipeline
    print("[INFO] Building feature engineering pipeline...")
    
    # StringIndexer + OneHotEncoder 处理类别特征
    indexers = []
    encoders = []
    for col in categorical_cols:
        indexer = StringIndexer(
            inputCol=col,
            outputCol=f"{col}_idx",
            handleInvalid="keep"   # 保留未见过的类别
        )
        encoder = OneHotEncoder(
            inputCols=[f"{col}_idx"],
            outputCols=[f"{col}_ohe"]
        )
        indexers.append(indexer)
        encoders.append(encoder)

    # 组合所有特征
    feature_cols = [f"{col}_ohe" for col in categorical_cols] + numeric_cols
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip"  # 跳过包含无效值的行
    )

    # 数据集分割
    train_data, test_data = df_ml.randomSplit([0.8, 0.2], seed=42)
    print(f"[INFO] Training samples: {train_data.count()}, Test samples: {test_data.count()}")

    # 构建并应用特征工程 Pipeline
    print("[INFO] Applying feature engineering...")
    feature_pipeline = Pipeline(stages=indexers + encoders + [assembler])
    feature_model = feature_pipeline.fit(train_data)
    train_data = feature_model.transform(train_data)
    test_data = feature_model.transform(test_data)

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
        labelCol="label",
        maxIter=100
    )

    # 2. Lasso 回归（L1 正则化）
    lasso = LinearRegression(
        featuresCol="features",
        labelCol="label",
        regParam=0.1,
        elasticNetParam=1.0,  # 1.0 = 纯 L1
        maxIter=100
    )

    # 3. Elastic Net（L1 + L2 正则化）
    elastic_net = LinearRegression(
        featuresCol="features",
        labelCol="label",
        regParam=0.1,
        elasticNetParam=0.5,  # 0.5 = L1 和 L2 混合
        maxIter=100
    )

    # 4. 决策树
    dt = DecisionTreeRegressor(
        featuresCol="features",
        labelCol="label",
        maxDepth=15,
        seed=42
    )

    # 5. 随机森林
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="label",
        numTrees=80,
        maxDepth=12,
        seed=42
    )

    # 6. 梯度提升树
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol="label",
        maxIter=50,
        maxDepth=6,
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
    evaluator_rmse = RegressionEvaluator(metricName="rmse", labelCol="label", predictionCol="prediction")
    evaluator_mae = RegressionEvaluator(metricName="mae", labelCol="label", predictionCol="prediction")
    evaluator_r2 = RegressionEvaluator(metricName="r2", labelCol="label", predictionCol="prediction")

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

        # 提取特征重要性（仅 Tree-based 模型）
        if model_name in ["decision_tree", "random_forest", "gbt"]:
            print(f"\n[INFO] 提取特征重要性...")
            
            try:
                importances = trained_model.featureImportances.toArray()
                
                # 获取特征名称
                feature_names = []
                for col in feature_cols:
                    if col.endswith("_ohe"):
                        # OHE 特征：获取原始列名
                        original_col = col.replace("_ohe", "")
                        # 获取编码后的维度数
                        ohe_size = train_data.select(col).head()[0].size
                        for i in range(ohe_size):
                            feature_names.append(f"{original_col}_{i}")
                    else:
                        # 数值特征
                        feature_names.append(col)
                
                # 配对特征名和重要性
                fi_pairs = list(zip(feature_names, importances))
                fi_sorted = sorted(fi_pairs, key=lambda x: x[1], reverse=True)
                
                # 显示 Top 15 特征
                print(f"\nTop 15 最重要特征:")
                for name, val in fi_sorted[:15]:
                    print(f"  {name:40s}: {val:.4f}")
                
                # 保存特征重要性
                fi_output = f"{args.output}/feature_importance/{model_name}"
                print(f"[INFO] 保存特征重要性到: {fi_output}")
                fi_df = spark.createDataFrame(fi_sorted, ["feature", "importance"])
                fi_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(fi_output)
                
            except Exception as e:
                print(f"[WARN] 无法提取特征重要性: {e}")

        # 保存模型
        model_path = f"{args.output}/models/{model_name}"
        print(f"[INFO] 保存模型到: {model_path}")
        trained_model.write().overwrite().save(model_path)

        # 保存预测样例
        predictions_sample = pred_df.select(
            "event_id",
            "artist",
            "genre",
            "city",
            "tm_min_price",
            F.col("label").alias("actual_price"),
            F.col("prediction").alias("predicted_price"),
            F.abs(F.col("label") - F.col("prediction")).alias("error")
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
    print(f"  - 特征重要性:   {args.output}/feature_importance/")
    print(f"  - 指标对比:     {args.output}/metrics_comparison_csv/")
    
    spark.stop()


if __name__ == "__main__":
    main()
