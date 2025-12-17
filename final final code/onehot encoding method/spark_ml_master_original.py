#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spark ML for master_df.csv - Ticket Price Prediction (Max)
  - The original version: using both one-hpt encoding categorical features

Features include:
  - Artist Spotify popularity
  - Genre and subgenre
  - City and state
  - Temporal features (year, month, weekday)
Target variable: tm_max_price

Usage:
Local:
    spark-submit spark_ml_master_original.py \
      --input output/master_parquet \
      --output output/ml_results_original

Dataproc:
    gcloud dataproc jobs submit pyspark spark_ml_master_original.py \
      --cluster=<cluster-name> \
      --region=us-east1 \
      -- --input gs://bucket/output/master_parquet \
         --output gs://bucket/output/ml_results_original
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
            F.col("tm_max_price").isNotNull() &
            (F.col("tm_max_price") > 0) &
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

    # Feature engineering
    print("Feature engineering...")
    
    # Categorical features
    categorical_cols = ["genre", "subgenre", "state", "city"]
    indexers = [
        StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep")
        for col in categorical_cols
    ]
    encoders = [
        OneHotEncoder(inputCol=f"{col}_idx", outputCol=f"{col}_vec")
        for col in categorical_cols
    ]

    # Numeric features
    numeric_features = ["spotify_popularity", "year", "month", "weekday"]

    # Assemble all features into a single vector
    feature_cols = numeric_features + [f"{col}_vec" for col in categorical_cols]    
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip"
    )
    
    # Select model
    print(f"Selected model type: {args.model_type}")
    if args.model_type == "rf":
        model = RandomForestRegressor(
            featuresCol="features",
            labelCol="tm_max_price",
            numTrees=100,
            maxDepth=10,
            seed=42
        )
    elif args.model_type == "gbt":
        model = GBTRegressor(
            featuresCol="features",
            labelCol="tm_max_price",
            maxIter=50,
            maxDepth=4,
            seed=42
        )
    else:  # lr
        model = LinearRegression(
            featuresCol="features",
            labelCol="tm_max_price",
            regParam=0.1,
            elasticNetParam=1.0,  
            maxIter=100
        )

    
    pipeline = Pipeline(stages=indexers + encoders + [assembler, model])


    # Split dataset
    print("Splitting data: 80% train, 20% test...")
    train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)
    print(f"  Train set: {train_df.count()}")
    print(f"  Test set: {test_df.count()}")

    # Train the model
    print("Training the model...")
    pipeline_model = pipeline.fit(train_df)

    # Make predictions
    predictions = pipeline_model.transform(test_df)

    # Evaluation
    print("\n Model Evaluation")

    evaluator_rmse = RegressionEvaluator(
        labelCol="tm_max_price",
        predictionCol="prediction",
        metricName="rmse"
    )
    evaluator_mae = RegressionEvaluator(
        labelCol="tm_max_price",
        predictionCol="prediction",
        metricName="mae"
    )
    evaluator_r2 = RegressionEvaluator(
        labelCol="tm_max_price",
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
        F.col("tm_max_price").alias("actual_price"),
        F.col("prediction").alias("predicted_price"),
        F.abs(F.col("tm_max_price") - F.col("prediction")).alias("error")
    ).show(20, truncate=False)

    # Save predictions
    predictions_output = f"{args.output}/predictions"
    print(f"\n Saving predictions to: {predictions_output}")
    predictions.select(
        "event_id",
        "artist",
        "genre",
        "event_date",
        "tm_max_price",
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

        # Determine pipeline stage indices
        num_indexers = len(categorical_cols)
        num_encoders = len(categorical_cols)
        encoder_models = pipeline_model.stages[num_indexers : num_indexers + num_encoders]

        # Build full feature name list
        expanded_feature_names = []

        # Numeric features (no expansion)
        for nf in numeric_features:
            expanded_feature_names.append(nf)

        # Categorical features (expanded by One-Hot Encoding)
        for col, encoder in zip(categorical_cols, encoder_models):
            size = encoder.categorySizes[0]  # Dimension after OHE
            for i in range(size):
                expanded_feature_names.append(f"{col}_{i}")

        # Validation: feature count must match model feature vector length
        model_feature_count = len(pipeline_model.stages[-1].featureImportances)
        if len(expanded_feature_names) != model_feature_count:
            print(
                f"WARNING: Feature name count = {len(expanded_feature_names)}, "
                f"but model has {model_feature_count} features."
            )
            print("   This indicates a mismatch in encoder dimensions or feature lists.")

        # Extract featureImportances
        feature_importance = list(pipeline_model.stages[-1].featureImportances)
        importance_list = list(zip(expanded_feature_names, feature_importance))

        # Sort by importance
        importance_list.sort(key=lambda x: x[1], reverse=True)

        # Print
        print("\nFeature importance ranking:")
        for feat, imp in importance_list:
            print(f"  {feat}: {imp:.5f}")

        # Save feature importance to CSV
        importance_output = f"{args.output}/feature_importance"
        importance_df = spark.createDataFrame(
            [(f, float(v)) for f, v in importance_list],
            ["feature", "importance"]
        )

        importance_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(importance_output)

        print(f" Feature importance saved to: {importance_output}")

    print("\n ML pipeline complete!")
    spark.stop()

if __name__ == "__main__":
    main()
