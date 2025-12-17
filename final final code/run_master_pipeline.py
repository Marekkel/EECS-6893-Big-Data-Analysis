"""
Data analysis pipeline of master_df.csv 

This script will sequentially run:
1. ETL: Data cleaning and transformation
2. Analytics: Multi-dimensional statistical analysis
3. ML: Machine learning price prediction

Usage:
    Local: python run_master_pipeline.py --mode local
    Dataproc: python run_master_pipeline.py --mode dataproc
"""

import os
import sys
import subprocess
import argparse
import json


def load_config():
    # Load Dataproc configuration
    if not os.path.exists("dataproc_config.json"):
        print("dataproc_config.json does not exist")
        sys.exit(1)
    
    with open("dataproc_config.json", "r") as f:
        return json.load(f)


def run_command(cmd, description):
    # Execute a command and show progress
    print(f"\n{description}")
    print(f"Command: {cmd}\n")
    
    result = subprocess.run(cmd, shell=True)
    
    if result.returncode == 0:
        print(f"\n{description} - Completed")
        return True
    else:
        print(f"\n{description} - Failed (Exit code: {result.returncode})")
        return False


def run_local():
    # Local execution mode
    print("\n Local Mode - Using master_df.csv")
    
    # Check data file
    if not os.path.exists("data/master_df.csv"):
        print("data/master_df.csv does not exist")
        return False
    
    print("Data file exists")
    
    # Step 1: ETL
    if not run_command(
        "spark-submit spark_etl_master.py --input data/master_df.csv --output output/master_parquet",
        "Step 1/6: ETL - Data cleaning and transformation"
    ):
        return False
    
    # Step 2: Analytics
    if not run_command(
        "spark-submit spark_analysis_master.py --input output/master_parquet --output output/analytics",
        "Step 2/6: Analytics - Multi-dimensional statistics"
    ):
        return False
    
    # Step 3: Single-model ML (MAX)
    if not run_command(
        "spark-submit spark_ml_master_max.py --input output/master_parquet --output output/ml_results_max --model-type rf",
        "Step 3/6: ML - Single-model price prediction (RandomForest) (MAX)"
    ):
        return False
    
    # Step 4: Multi-model comparison training (MAX)
    if not run_command(
        "spark-submit spark_ml_multi_models_max.py --input output/master_parquet --output output/ml_multi_models_max",
        "Step 4/6: ML - Multi-model comparison training (6 models) (MAX)"
    ):
        return False
    
    # Step 5: Single-model ML (MIN)
    if not run_command(
        "spark-submit spark_ml_master_min.py --input output/master_parquet --output output/ml_results_min --model-type rf",
        "Step 5/6: ML - Single-model price prediction (RandomForest) (MIN)"
    ):
        return False
    
    # Step 6: Multi-model comparison training (MIN)
    if not run_command(
        "spark-submit spark_ml_multi_models_min.py --input output/master_parquet --output output/ml_multi_models_min",
        "Step 6/6: ML - Multi-model comparison training (6 models) (MIN)"
    ):
        return False
    
    print("\n Local pipeline completed!")
    print("\n Output locations:")
    print("  - ETL output: output/master_parquet/")
    print("  - Analytics results: output/analytics/")
    print("  - Single-model ML (MAX): output/ml_results_max/")
    print("  - Multi-model comparison (MAX): output/ml_multi_models_max/")
    print("  - Single-model ML (MIN): output/ml_results_min/")
    print("  - Multi-model comparison (MIN): output/ml_multi_models_min/")
    
    return True


def run_dataproc():
    # Dataproc execution mode
    print("\n Dataproc Mode - Using master_df.csv")
    
    config = load_config()
    project = config['project_id']
    region = config['region']
    cluster = config['cluster_name']
    bucket = config['bucket_name']
    
    print(f"\n Configuration:")
    print(f"  Project: {project}")
    print(f"  Region: {region}")
    print(f"  Cluster: {cluster}")
    print(f"  Bucket: {bucket}")
    
    # Step 1: Upload data and scripts
    print("\n Uploading files to GCS...")
    
    uploads = [
        ("data/master_df.csv", f"gs://{bucket}/data/master_df.csv"),
        ("spark_etl_master.py", f"gs://{bucket}/scripts/spark_etl_master.py"),
        ("spark_analysis_master.py", f"gs://{bucket}/scripts/spark_analysis_master.py"),
        ("spark_ml_master_max.py", f"gs://{bucket}/scripts/spark_ml_master_max.py"),
        ("spark_ml_multi_models_max.py", f"gs://{bucket}/scripts/spark_ml_multi_models_max.py"),
        ("spark_ml_master_min.py", f"gs://{bucket}/scripts/spark_ml_master_min.py"),
        ("spark_ml_multi_models_min.py", f"gs://{bucket}/scripts/spark_ml_multi_models_min.py")
    ]
    
    for local_file, gcs_path in uploads:
        if not os.path.exists(local_file):
            print(f"{local_file} does not exist")
            return False
        
        cmd = f"gsutil cp {local_file} {gcs_path}"
        if not run_command(cmd, f"Upload {local_file}"):
            return False
    
    # Step 2: Submit ETL job
    etl_cmd = f"""gcloud dataproc jobs submit pyspark gs://{bucket}/scripts/spark_etl_master.py \
        --cluster={cluster} \
        --region={region} \
        --project={project} \
        -- --input gs://{bucket}/data/master_df.csv \
           --output gs://{bucket}/output/master_parquet"""
    
    if not run_command(etl_cmd, "Step 1/6: Dataproc ETL job"):
        return False
    
    # Step 3: Submit Analytics job
    analysis_cmd = f"""gcloud dataproc jobs submit pyspark gs://{bucket}/scripts/spark_analysis_master.py \
        --cluster={cluster} \
        --region={region} \
        --project={project} \
        -- --input gs://{bucket}/output/master_parquet \
           --output gs://{bucket}/output/analytics"""
    
    if not run_command(analysis_cmd, "Step 2/6: Dataproc Analytics job"):
        return False
    
    # Step 4: Submit single-model ML job (MAX)
    ml_cmd = f"""gcloud dataproc jobs submit pyspark gs://{bucket}/scripts/spark_ml_master_max.py \
        --cluster={cluster} \
        --region={region} \
        --project={project} \
        -- --input gs://{bucket}/output/master_parquet \
           --output gs://{bucket}/output/ml_results_max \
           --model-type rf"""
    
    if not run_command(ml_cmd, "Step 3/6: Dataproc single-model ML job (MAX)"):
        return False
    
    # Step 5: Submit multi-model comparison job (MAX)
    multi_ml_cmd = f"""gcloud dataproc jobs submit pyspark gs://{bucket}/scripts/spark_ml_multi_models_max.py \
        --cluster={cluster} \
        --region={region} \
        --project={project} \
        -- --input gs://{bucket}/output/master_parquet \
           --output gs://{bucket}/output/ml_multi_models_max"""
    
    if not run_command(multi_ml_cmd, "Step 4/6: Dataproc multi-model comparison (MAX)"):
        return False

    # Step 6: Submit single-model ML job (MIN)
    ml_cmd = f"""gcloud dataproc jobs submit pyspark gs://{bucket}/scripts/spark_ml_master_min.py \
        --cluster={cluster} \
        --region={region} \
        --project={project} \
        -- --input gs://{bucket}/output/master_parquet \
           --output gs://{bucket}/output/ml_results_min \
           --model-type rf"""
    
    if not run_command(ml_cmd, "Step 5/6: Dataproc single-model ML job (MIN)"):
        return False
    
    # Step 7: Submit multi-model comparison job (MIN)
    multi_ml_cmd = f"""gcloud dataproc jobs submit pyspark gs://{bucket}/scripts/spark_ml_multi_models_min.py \
        --cluster={cluster} \
        --region={region} \
        --project={project} \
        -- --input gs://{bucket}/output/master_parquet \
           --output gs://{bucket}/output/ml_multi_models_min"""
    
    if not run_command(multi_ml_cmd, "Step 6/6: Dataproc multi-model comparison (MIN)"):
        return False
    
    print("\n Dataproc pipeline completed!")
    print(f"\n GCS output locations:")
    print(f"  - ETL output: gs://{bucket}/output/master_parquet/")
    print(f"  - Analytics results: gs://{bucket}/output/analytics/")
    print(f"  - Single-model ML (MAX): gs://{bucket}/output/ml_results_max/")
    print(f"  - Multi-model comparison (MAX): gs://{bucket}/output/ml_multi_models_max/")
    print(f"  - Single-model ML (MIN): gs://{bucket}/output/ml_results_min/")
    print(f"  - Multi-model comparison (MIN): gs://{bucket}/output/ml_multi_models_min/")
    print(f"\n To download results:")
    print(f"  gsutil -m cp -r gs://{bucket}/output/ ./")
    
    return True


def main():
    parser = argparse.ArgumentParser(description="Run the complete master_df.csv analysis pipeline")
    parser.add_argument(
        "--mode",
        type=str,
        required=True,
        choices=["local", "dataproc"],
        help="Execution mode: local (on-premise) or dataproc (cloud)"
    )
    
    args = parser.parse_args()
    
    if args.mode == "local":
        success = run_local()
    else:
        success = run_dataproc()
    
    if success:
        print("\n All steps completed successfully!")
        sys.exit(0)
    else:
        print("\n Pipeline execution failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
    

