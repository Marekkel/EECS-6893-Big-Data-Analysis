#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spark ML for master_df.csv - Ticket Price Prediction (MIN)

Predict primary market prices for events using machine learning.
Features include:
  - Artist Spotify popularity
  - Genre and subgenre
  - City and state
  - Temporal features (year, month, weekday)
Target variable: tm_min_price

Usage:
Local:
    spark-submit spark_ml_master_min.py \
      --input ../output/master_parquet \
      --output ../output/ml_results_min

Dataproc:
    gcloud dataproc jobs submit pyspark spark_ml_master_min.py \
      --cluster=<cluster-name> \
      --region=us-east1 \
      -- --input gs://bucket/output/master_parquet \
         --output gs://bucket/output/ml_results_min
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

    print(f"Reading processed data from: {args.input}")
    df = spark.read.parquet(args.input)

    # Data preparation
    print("Preparing data for ML...")
    df_ml = (
        df.filter(
            F.col("tm_min_price").isNotNull() &
            (F.col("tm_min_price") >= 0) &
            F.col("artist").isNotNull()
        )
        # Fill missing values
        .fillna({
            "spotify_popularity": 0,
            "genre": "Unknown",
            "subgenre": "Unknown",
            "city": "Unknown",
            "state": "Unknown"
        })
    )
    print(f"ML dataset size: {df_ml.count()}")
    
    # Split dataset
    print("Splitting data: 80% train, 20% test...")
    train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)
    print(f"  Train set: {train_df.count()}")
    print(f"  Test set: {test_df.count()}")

    # Replace categorical labels with mean price to avoid high-cardinality issues
    # Global average used as fallback for unseen categories
    global_avg = train_df.select(F.avg("tm_min_price")).first()[0]  

    def add_avg_encoding(df, ref_df, col):
        avg_df = (
            ref_df
            .groupBy(col)
            .agg(F.avg("tm_min_price").alias(f"{col}_avg_price"))
        )
        return df.join(avg_df, on=col, how="left") \
                .fillna({f"{col}_avg_price": global_avg})        
                
    # Compute category averages using training data 
    for c in ["state", "city", "genre", "subgenre"]:
        train_df = add_avg_encoding(train_df, train_df, c)
        test_df  = add_avg_encoding(test_df,  train_df, c)
        
    # Fill remaining null values
    avg_cols = ["state_avg_price", "city_avg_price", "genre_avg_price", "subgenre_avg_price"]
    train_df = train_df.fillna(0, subset=avg_cols)
    test_df  = test_df.fillna(0, subset=avg_cols)
    
    # Save average encoding results
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
        
    # Save global average
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

    # Feature engineering
    print("Feature engineering...")
    
    # Numeric features
    numeric_features = ["spotify_popularity", "year", "month", "weekday", 
                        "state_avg_price", "city_avg_price", "genre_avg_price", "subgenre_avg_price"]

    # Assemble all features into a single vector
    assembler = VectorAssembler(
        inputCols=numeric_features,
        outputCol="features",
        handleInvalid="error"
    )
        
    # Select model
    print(f"Selected model type: {args.model_type}")
    if args.model_type == "rf":
        model = RandomForestRegressor(
            featuresCol="features",
            labelCol="tm_min_price",
            numTrees=100,
            maxDepth=20,
            seed=42
        )
    elif args.model_type == "gbt":
        model = GBTRegressor(
            featuresCol="features",
            labelCol="tm_min_price",
            maxIter=50,
            maxDepth=2,
            seed=42
        )
    else:  # lr
        model = LinearRegression(
            featuresCol="features",
            labelCol="tm_min_price",
            regParam=0.1,
            elasticNetParam=1.0,  
            maxIter=100
        )

    pipeline = Pipeline(stages=[assembler, model])

    # Train the model
    print("Training the model...")
    pipeline_model = pipeline.fit(train_df)

    # Make predictions
    predictions = pipeline_model.transform(test_df)

    # Evaluation
    print("\n Model Evaluation")

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

    print(f"RMSE (Root Mean Squared Error): {rmse:.2f}")
    print(f"MAE (Mean Absolute Error): {mae:.2f}")
    print(f"R² (R-squared): {r2:.4f}")

    # Show sample predictions
    print("\n Sample predictions (first 20 rows):")
    predictions.select(
        "artist",
        "genre",
        F.col("tm_min_price").alias("actual_price"),
        F.col("prediction").alias("predicted_price"),
        F.abs(F.col("tm_min_price") - F.col("prediction")).alias("error")
    ).show(20, truncate=False)

    # Save predictions
    predictions_output = f"{args.output}/predictions"
    print(f"\n Saving predictions to: {predictions_output}")
    predictions.select(
        "event_id",
        "artist",
        "genre",
        "event_date",
        "tm_min_price",
        "prediction",
        "spotify_popularity"
    ).coalesce(5).write.mode("overwrite").option("header", "true").csv(predictions_output)

    # Save evaluation metrics
    metrics_output = f"{args.output}/metrics"
    print(f" Saving metrics to: {metrics_output}")
    
    # Ensure all values are float
    metrics_data = [
        ("RMSE", float(rmse)),
        ("MAE", float(mae)),
        ("R2", float(r2)),
        ("train_size", float(train_df.count())),
        ("test_size", float(test_df.count()))
    ]
    metrics_df = spark.createDataFrame(metrics_data, ["metric", "value"])
    metrics_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(metrics_output)

    # Feature importance (only for RandomForest and GBT)
    if args.model_type in ["rf", "gbt"]:
        print("\n Extracting feature importance...")

        feature_importance = list(pipeline_model.stages[-1].featureImportances)

        # Numeric feature names 
        feature_names = numeric_features

        # Safety check
        if len(feature_importance) != len(feature_names):
            print(
                f"WARNING: feature importance length = {len(feature_importance)}, "
                f"but feature names length = {len(feature_names)}"
            )

        importance_list = list(zip(feature_names, feature_importance))
        importance_list.sort(key=lambda x: x[1], reverse=True)

        print("\n Feature importance ranking:")
        for feat, imp in importance_list:
            print(f"  {feat}: {imp:.5f}")

        # Save as CSV
        importance_output = f"{args.output}/feature_importance"
        importance_df = spark.createDataFrame(
            [(f, float(v)) for f, v in importance_list],
            ["feature", "importance"]
        )

        importance_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(importance_output)
        print(f" Feature importance saved to: {importance_output}")
        
    print("\n ML pipeline finished successfully!")
    spark.stop()


if __name__ == "__main__":
    main()