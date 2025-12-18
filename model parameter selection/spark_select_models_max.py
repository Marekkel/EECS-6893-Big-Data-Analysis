#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spark Multi-Model Parameter Selection - master_df.csv data (MAX)

Train six regression models to find the best parameter for each model:
  1. Linear Regression
  2. Lasso Regression (L1 regularization)
  3. Elastic Net (L1 + L2 regularization)
  4. Decision Tree
  5. Random Forest
  6. Gradient Boosted Tree

Due to Spark's limited computational capacity, these models need to be trained in multiple separate runs.
For convenience, they are implemented within a single script.

Usage:
Local run:
    spark-submit spark_select_models_max.py \
      --input output/master_parquet \
      --output output/select_models_max

Dataproc:
    gcloud dataproc jobs submit pyspark spark_select_models_max.py \
      --cluster=<cluster-name> \
      --region=us-east1 \
      -- --input gs://bucket/output/master_parquet \
         --output gs://bucket/output/select_models_max
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
        help="Path to input Parquet files from ETL step"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Base path to save models and evaluation metrics"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    spark = build_spark()

    print(f"Reading processed data from: {args.input}")
    df = spark.read.parquet(args.input)
    print("Schema:")
    df.printSchema()
    
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
    
    # Split dataset
    print("Splitting data: 80% train, 20% test...")
    train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)
    print(f"  Train set size: {train_df.count()}")
    print(f"  Test set size: {test_df.count()}")

    # Replace categorical labels with mean price to avoid high-cardinality issues
    # Global average used as fallback for unseen categories
    global_avg = train_df.select(F.avg("tm_max_price")).first()[0]  

    def add_avg_encoding(df, ref_df, col):
        avg_df = (
            ref_df
            .groupBy(col)
            .agg(F.avg("tm_max_price").alias(f"{col}_avg_price"))
        )
        return (
            df.join(avg_df, on=col, how="left")
              .fillna({f"{col}_avg_price": global_avg})
        )
    
    # Compute category averages using training data 
    for c in ["state", "city", "genre", "subgenre"]:
        train_df = add_avg_encoding(train_df, train_df, c)
        test_df  = add_avg_encoding(test_df,  train_df, c)
        
    # Fill remaining null values
    avg_cols = ["state_avg_price", "city_avg_price", "genre_avg_price", "subgenre_avg_price"]
    train_df = train_df.fillna(0, subset=avg_cols)
    test_df  = test_df.fillna(0, subset=avg_cols)
    
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
    
    # Build and apply feature engineering pipeline
    print("Applying feature engineering...")
    pipeline = Pipeline(stages=[assembler])
    feature_model = pipeline.fit(train_df)
    train_data = feature_model.transform(train_df)
    test_data = feature_model.transform(test_df)

    # Cache datasets to speed up multiple model trainings
    train_data = train_data.cache()
    test_data = test_data.cache()

    print(f"Feature vector size: {train_data.select('features').head()[0].size}")
    
    
    # Define six regression models
    print("\n Defining multiple regression models")

    # 1. Standard linear regression
    lr_1 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        maxIter=100
    )
    
    lr_2 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        maxIter=200
    )

    # 2. Lasso regression (L1 regularization)
    lasso_1 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.1,
        elasticNetParam=1.0,  
        maxIter=100
    )

    lasso_2 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.01,
        elasticNetParam=1.0,  
        maxIter=100
    )
    
    lasso_3 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.05,
        elasticNetParam=1.0,  
        maxIter=100
    )

    lasso_4 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.1,
        elasticNetParam=1.0,  
        maxIter=200
    )

    lasso_5 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.01,
        elasticNetParam=1.0,  
        maxIter=200
    )
    
    lasso_6 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.05,
        elasticNetParam=1.0, 
        maxIter=200
    )

    # 3. Elastic Net (L1 + L2 regularization)
    elastic_net_1 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.1,
        elasticNetParam=0.5,   
        maxIter=100
    )

    elastic_net_2 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.05,
        elasticNetParam=0.5,   
        maxIter=100
    )

    elastic_net_3 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.01,
        elasticNetParam=0.5,   
        maxIter=100
    )
    
    elastic_net_4 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.1,
        elasticNetParam=0.3,   
        maxIter=100
    )

    elastic_net_5 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.05,
        elasticNetParam=0.3,   
        maxIter=100
    )

    elastic_net_6 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.01,
        elasticNetParam=0.3,   
        maxIter=100
    )
    
    elastic_net_7 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.1,
        elasticNetParam=0.7,   
        maxIter=100
    )

    elastic_net_8 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.05,
        elasticNetParam=0.7,   
        maxIter=100
    )

    elastic_net_9 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.01,
        elasticNetParam=0.7,   
        maxIter=100
    )

    elastic_net_10 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.1,
        elasticNetParam=0.5,   
        maxIter=200
    )

    elastic_net_11 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.05,
        elasticNetParam=0.5,   
        maxIter=200
    )

    elastic_net_12 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.01,
        elasticNetParam=0.3,   
        maxIter=200
    )
    
    elastic_net_13 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.1,
        elasticNetParam=0.3,   
        maxIter=200
    )

    elastic_net_14 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.05,
        elasticNetParam=0.3,   
        maxIter=200
    )

    elastic_net_15 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.01,
        elasticNetParam=0.3,   
        maxIter=200
    )
    
    elastic_net_16 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.1,
        elasticNetParam=0.7,   
        maxIter=200
    )

    elastic_net_17 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.05,
        elasticNetParam=0.7,   
        maxIter=200
    )

    elastic_net_18 = LinearRegression(
        featuresCol="features",
        labelCol="tm_max_price",
        regParam=0.01,
        elasticNetParam=0.7,   
        maxIter=200
    )

    # 4. Decision tree
    dt_1 = DecisionTreeRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        maxDepth=6,
        seed=42
    )

    dt_2 = DecisionTreeRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        maxDepth=8,
        seed=42
    )
    
    dt_3 = DecisionTreeRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        maxDepth=10,
        seed=42
    )
    
    dt_4 = DecisionTreeRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        maxDepth=12,
        seed=42
    )

    dt_5 = DecisionTreeRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        maxDepth=15,
        seed=42
    )

    dt_6 = DecisionTreeRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        maxDepth=20,
        seed=42
    )

    # 5. Random forest
    rf_1 = RandomForestRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        numTrees=100,
        maxDepth=6,
        seed=42
    )

    rf_2 = RandomForestRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        numTrees=100,
        maxDepth=10,
        seed=42
    )
    rf_3 = RandomForestRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        numTrees=100,
        maxDepth=15,
        seed=42
    )
    rf_4 = RandomForestRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        numTrees=100,
        maxDepth=20,
        seed=42
    )
    rf_5 = RandomForestRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        numTrees=200,
        maxDepth=8,
        seed=42
    )
    rf_6 = RandomForestRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        numTrees=200,
        maxDepth=10,
        seed=42
    )
    rf_7 = RandomForestRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        numTrees=200,
        maxDepth=15,
        seed=42
    )
    rf_8 = RandomForestRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        numTrees=200,
        maxDepth=20,
        seed=42
    )

    # 6. Gradient boosting tree
    gbt_1 = GBTRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        maxIter=100,
        maxDepth=6,
        seed=42
    )
    gbt_2 = GBTRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        maxIter=100,
        maxDepth=10,
        seed=42
    )
    gbt_3 = GBTRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        maxIter=100,
        maxDepth=4,
        seed=42
    )

    gbt_4 = GBTRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        maxIter=50,
        maxDepth=4,
        seed=42
    )

    gbt_5 = GBTRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        maxIter=100,
        maxDepth=2,
        seed=42
    )

    gbt_6 = GBTRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        maxIter=50,
        maxDepth=2,
        seed=42
    )

    gbt_7 = GBTRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        maxIter=50,
        maxDepth=6,
        seed=42
    )

    gbt_8 = GBTRegressor(
        featuresCol="features",
        labelCol="tm_max_price",
        maxIter=50,
        maxDepth=10,
        seed=42
    )
    
    regression_models = {
        "lr_1": lr_1,
        "lr_2": lr_2,
        "lasso_1": lasso_1,
        "lasso_2": lasso_2,
        "lasso_3": lasso_3,
        "lasso_4": lasso_4,
        "lasso_5": lasso_5,
        "lasso_6": lasso_6,
        "elastic_net_1": elastic_net_1,
        "elastic_net_2": elastic_net_2,
        "elastic_net_3": elastic_net_3,
        "elastic_net_4": elastic_net_4,
        "elastic_net_5": elastic_net_5,
        "elastic_net_6": elastic_net_6,
        "elastic_net_7": elastic_net_7,
        "elastic_net_8": elastic_net_8,
        "elastic_net_9": elastic_net_9,
        "elastic_net_10": elastic_net_10,
        "elastic_net_11": elastic_net_11,
        "elastic_net_12": elastic_net_12,
        "elastic_net_13": elastic_net_13,
        "elastic_net_14": elastic_net_14,
        "elastic_net_15": elastic_net_15,
        "elastic_net_16": elastic_net_16,
        "elastic_net_17": elastic_net_17,
        "elastic_net_18": elastic_net_18,
        "dt_1": dt_1,
        "dt_2": dt_2,
        "dt_3": dt_3,
        "dt_4": dt_4,
        "dt_5": dt_5,
        "dt_6": dt_6,
        "rf_1": rf_1,
        "rf_2": rf_2,
        "rf_3": rf_3,
        "rf_4": rf_4,
        "rf_5": rf_5,
        "rf_6": rf_6,
        "rf_7": rf_7,
        "rf_8": rf_8,
        "gbt_1": gbt_1,
        "gbt_2": gbt_2,
        "gbt_3": gbt_3,
        "gbt_4": gbt_4,
        "gbt_5": gbt_5,
        "gbt_6": gbt_6,
        "gbt_7": gbt_7,
        "gbt_8": gbt_8
    }


    print(f"Training {len(regression_models)} regression models")

    # Train and evaluate all models
    evaluator_rmse = RegressionEvaluator(
        metricName="rmse",
        labelCol="tm_max_price",
        predictionCol="prediction"
    )
    evaluator_mae = RegressionEvaluator(
        metricName="mae",
        labelCol="tm_max_price",
        predictionCol="prediction"
    )
    evaluator_r2 = RegressionEvaluator(
        metricName="r2",
        labelCol="tm_max_price",
        predictionCol="prediction"
    )

    metrics_list = []
    
    for model_name, model in regression_models.items():
        print(f"\n Training model: {model_name}")

        # Train model
        trained_model = model.fit(train_data)
        
        # Generate predictions
        pred_df = trained_model.transform(test_data)

        # Evaluate
        print("\n Model Evaluation")
        rmse = evaluator_rmse.evaluate(pred_df)
        mae = evaluator_mae.evaluate(pred_df)
        r2 = evaluator_r2.evaluate(pred_df)
        
        print(f"RMSE (Root Mean Squared Error): {rmse:.2f}")
        print(f"MAE (Mean Absolute Error): {mae:.2f}")
        print(f"R² (R-squared): {r2:.4f}")

        # Save trained model
        model_path = f"{args.output}/models/{model_name}"
        print(f"Saving model to: {model_path}")
        trained_model.write().overwrite().save(model_path)

        # Save prediction samples
        predictions_sample = pred_df.select(
            "event_id",
            "artist",
            "genre",
            F.col("tm_max_price").alias("actual_price"),
            F.col("prediction").alias("predicted_price"),
            F.abs(F.col("tm_max_price") - F.col("prediction")).alias("error")
        ).limit(100)
        
        sample_output = f"{args.output}/predictions_sample/{model_name}"
        print(f"Saving prediction samples to: {sample_output}")
        predictions_sample.coalesce(1).write.mode("overwrite").option("header", "true").csv(sample_output)

        # Record metrics
        metrics_list.append((model_name, float(rmse), float(mae), float(r2)))

    # Save model comparison metrics
    metrics_output = f"{args.output}/metrics_comparison"
    print(f"\n Saving model comparison metrics to: {metrics_output}")
    
    metrics_schema = ["model", "rmse", "mae", "r2"]
    metrics_df = spark.createDataFrame(metrics_list, metrics_schema)
    
    # Save as CSV
    metrics_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(metrics_output + "_csv")
    # Save as JSON
    metrics_df.coalesce(1).write.mode("overwrite").json(metrics_output + "_json")

    # Display comparison results
    print("\n Model Performance Comparison")
    metrics_df.orderBy("rmse").show(truncate=False)

    # Identify the best model
    best_model = metrics_df.orderBy("rmse").first()
    print(f"\n Best model: {best_model['model']}")
    print(f"   RMSE: ${best_model['rmse']:.2f}")
    print(f"   MAE:  ${best_model['mae']:.2f}")
    print(f"   R²:   {best_model['r2']:.4f}")

    print("\n Multi-model training complete.")
    print("\n Output locations:")
    print(f"  - Models:              {args.output}/models/")
    print(f"  - Prediction samples:  {args.output}/predictions_sample/")
    print(f"  - Metrics comparison:  {args.output}/metrics_comparison_csv/")
    
    spark.stop()


if __name__ == "__main__":
    main()