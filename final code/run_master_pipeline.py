"""
完整的 master_df.csv 数据分析流程

这个脚本会依次运行:
1. ETL: 数据清洗和转换
2. Analytics: 多维度统计分析
3. ML: 机器学习价格预测

使用方法:
    本地运行: python run_master_pipeline.py --mode local
    Dataproc: python run_master_pipeline.py --mode dataproc
"""

import os
import sys
import subprocess
import argparse
import json


def load_config():
    """加载 Dataproc 配置"""
    if not os.path.exists("dataproc_config.json"):
        print("❌ dataproc_config.json 不存在")
        sys.exit(1)
    
    with open("dataproc_config.json", "r") as f:
        return json.load(f)


def run_command(cmd, description):
    """运行命令并显示进度"""
    print(f"\n{'='*80}")
    print(f"🚀 {description}")
    print(f"{'='*80}")
    print(f"命令: {cmd}\n")
    
    result = subprocess.run(cmd, shell=True)
    
    if result.returncode == 0:
        print(f"\n✅ {description} - 完成")
        return True
    else:
        print(f"\n❌ {description} - 失败 (退出码: {result.returncode})")
        return False


def run_local():
    """本地运行模式"""
    print("\n" + "="*80)
    print("📍 本地模式 - 使用 master_df.csv")
    print("="*80)
    
    # 检查数据文件
    if not os.path.exists("data/master_df.csv"):
        print("❌ data/master_df.csv 不存在")
        return False
    
    print("✅ 数据文件存在")
    
    # Step 1: ETL
    if not run_command(
        "spark-submit spark_etl_master.py --input data/master_df.csv --output output/master_parquet",
        "步骤 1/3: ETL - 数据清洗与转换"
    ):
        return False
    
    # Step 2: Analytics
    if not run_command(
        "spark-submit spark_analysis_master.py --input output/master_parquet --output output/analytics",
        "步骤 2/3: 分析 - 多维度统计"
    ):
        return False
    
    # Step 3: 单模型 ML(MAX)
    if not run_command(
        "spark-submit spark_ml_master_max.py --input output/master_parquet --output output/ml_results_max --model-type rf",
        "步骤 3/4: 机器学习 - 单模型价格预测 (RandomForest)(MAX)"
    ):
        return False
    
    # Step 4: 多模型对比训练(MAX)
    if not run_command(
        "spark-submit spark_ml_multi_models_max.py --input output/master_parquet --output output/ml_multi_models_max",
        "步骤 4/4: 机器学习 - 多模型对比训练 (6种模型)(MAX)"
    ):
        return False
    
    # Step 5: 单模型 ML(MIN)
    if not run_command(
        "spark-submit spark_ml_master_min.py --input output/master_parquet --output output/ml_results_min --model-type rf",
        "步骤 3/4: 机器学习 - 单模型价格预测 (RandomForest)(MIN)"
    ):
        return False
    
    # Step 6: 多模型对比训练(MIN)
    if not run_command(
        "spark-submit spark_ml_multi_models_min.py --input output/master_parquet --output output/ml_multi_models_min",
        "步骤 4/4: 机器学习 - 多模型对比训练 (6种模型)(MIN)"
    ):
        return False
    
    print("\n" + "="*80)
    print("✅ 本地流程完成！")
    print("="*80)
    print("\n📁 结果位置:")
    print("  - ETL 输出: output/master_parquet/")
    print("  - 分析结果: output/analytics/")
    print("  - 单模型 ML(MAX): output/ml_results_max/")
    print("  - 多模型对比(MAX): output/ml_multi_models_max/")
    print("  - 单模型 ML(MIN): output/ml_results_min/")
    print("  - 多模型对比(MIN): output/ml_multi_models_min/")
    
    return True


def run_dataproc():
    """Dataproc 运行模式"""
    print("\n" + "="*80)
    print("☁️  Dataproc 模式 - 使用 master_df.csv")
    print("="*80)
    
    config = load_config()
    project = config['project_id']
    region = config['region']
    cluster = config['cluster_name']
    bucket = config['bucket_name']
    
    print(f"\n配置信息:")
    print(f"  项目: {project}")
    print(f"  区域: {region}")
    print(f"  集群: {cluster}")
    print(f"  存储桶: {bucket}")
    
    # Step 1: 上传数据和脚本
    print("\n📤 上传文件到 GCS...")
    
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
            print(f"❌ {local_file} 不存在")
            return False
        
        cmd = f"gsutil cp {local_file} {gcs_path}"
        if not run_command(cmd, f"上传 {local_file}"):
            return False
    
    # Step 2: 提交 ETL 作业
    etl_cmd = f"""gcloud dataproc jobs submit pyspark gs://{bucket}/scripts/spark_etl_master.py \
        --cluster={cluster} \
        --region={region} \
        --project={project} \
        -- --input gs://{bucket}/data/master_df.csv \
           --output gs://{bucket}/output/master_parquet"""
    
    if not run_command(etl_cmd, "步骤 1/4: Dataproc ETL 作业"):
        return False
    
    # Step 3: 提交分析作业
    analysis_cmd = f"""gcloud dataproc jobs submit pyspark gs://{bucket}/scripts/spark_analysis_master.py \
        --cluster={cluster} \
        --region={region} \
        --project={project} \
        -- --input gs://{bucket}/output/master_parquet \
           --output gs://{bucket}/output/analytics"""
    
    if not run_command(analysis_cmd, "步骤 2/4: Dataproc 分析作业"):
        return False
    
    # Step 4: 提交单模型 ML 作业(MAX)
    ml_cmd = f"""gcloud dataproc jobs submit pyspark gs://{bucket}/scripts/spark_ml_master_max.py \
        --cluster={cluster} \
        --region={region} \
        --project={project} \
        -- --input gs://{bucket}/output/master_parquet \
           --output gs://{bucket}/output/ml_results_max \
           --model-type rf"""
    
    if not run_command(ml_cmd, "步骤 3/4: Dataproc 单模型 ML 作业(MAX)"):
        return False
    
    # Step 5: 提交多模型对比训练作业(MAX)
    multi_ml_cmd = f"""gcloud dataproc jobs submit pyspark gs://{bucket}/scripts/spark_ml_multi_models_max.py \
        --cluster={cluster} \
        --region={region} \
        --project={project} \
        -- --input gs://{bucket}/output/master_parquet \
           --output gs://{bucket}/output/ml_multi_models_max"""
    
    if not run_command(multi_ml_cmd, "步骤 4/4: Dataproc 多模型对比训练(MAX)"):
        return False

    # Step 6: 提交单模型 ML 作业(MIN)
    ml_cmd = f"""gcloud dataproc jobs submit pyspark gs://{bucket}/scripts/spark_ml_master_min.py \
        --cluster={cluster} \
        --region={region} \
        --project={project} \
        -- --input gs://{bucket}/output/master_parquet \
           --output gs://{bucket}/output/ml_results_min \
           --model-type rf"""
    
    if not run_command(ml_cmd, "步骤 3/4: Dataproc 单模型 ML 作业(MIN)"):
        return False
    
    # Step 7: 提交多模型对比训练作业(MIN)
    multi_ml_cmd = f"""gcloud dataproc jobs submit pyspark gs://{bucket}/scripts/spark_ml_multi_models_min.py \
        --cluster={cluster} \
        --region={region} \
        --project={project} \
        -- --input gs://{bucket}/output/master_parquet \
           --output gs://{bucket}/output/ml_multi_models_min"""
    
    if not run_command(multi_ml_cmd, "步骤 4/4: Dataproc 多模型对比训练(MIN)"):
        return False
    
    print("\n" + "="*80)
    print("✅ Dataproc 流程完成！")
    print("="*80)
    print(f"\n📁 GCS 结果位置:")
    print(f"  - ETL 输出: gs://{bucket}/output/master_parquet/")
    print(f"  - 分析结果: gs://{bucket}/output/analytics/")
    print(f"  - 单模型 ML(MAX): gs://{bucket}/output/ml_results_max/")
    print(f"  - 多模型对比(MAX): gs://{bucket}/output/ml_multi_models_max/")
    print(f"  - 单模型 ML(MIN): gs://{bucket}/output/ml_results_min/")
    print(f"  - 多模型对比(MIN): gs://{bucket}/output/ml_multi_models_min/")
    print(f"\n💡 下载结果:")
    print(f"  gsutil -m cp -r gs://{bucket}/output/ ./")
    
    return True


def main():
    parser = argparse.ArgumentParser(description="运行 master_df.csv 完整分析流程")
    parser.add_argument(
        "--mode",
        type=str,
        required=True,
        choices=["local", "dataproc"],
        help="运行模式: local (本地) 或 dataproc (云端)"
    )
    
    args = parser.parse_args()
    
    if args.mode == "local":
        success = run_local()
    else:
        success = run_dataproc()
    
    if success:
        print("\n🎉 所有步骤成功完成！")
        sys.exit(0)
    else:
        print("\n❌ 流程执行失败")
        sys.exit(1)


if __name__ == "__main__":
    main()
