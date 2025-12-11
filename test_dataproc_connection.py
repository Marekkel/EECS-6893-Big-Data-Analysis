"""
验证 Dataproc 配置和连接

使用方法: python test_dataproc_connection.py
"""

import json
import subprocess
import sys


def test_gcloud():
    """测试 gcloud CLI"""
    print("\n1️⃣  测试 gcloud CLI...")
    result = subprocess.run("gcloud --version", shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        print("✅ gcloud CLI 已安装")
        print(result.stdout[:200])
        return True
    else:
        print("❌ gcloud CLI 未安装或未配置")
        return False


def test_gsutil():
    """测试 gsutil"""
    print("\n2️⃣  测试 gsutil...")
    result = subprocess.run("gsutil version", shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        print("✅ gsutil 已安装")
        return True
    else:
        print("❌ gsutil 未安装")
        return False


def load_config():
    """加载配置"""
    print("\n3️⃣  加载配置文件...")
    try:
        with open("dataproc_config.json", 'r') as f:
            config = json.load(f)
        print("✅ 配置文件已加载")
        return config
    except FileNotFoundError:
        print("❌ dataproc_config.json 不存在")
        return None


def test_gcs_access(bucket_name):
    """测试 GCS 访问"""
    print(f"\n4️⃣  测试 GCS Bucket 访问: gs://{bucket_name}")
    result = subprocess.run(
        f"gsutil ls gs://{bucket_name}/",
        shell=True,
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        print(f"✅ 可以访问 gs://{bucket_name}")
        return True
    else:
        print(f"❌ 无法访问 gs://{bucket_name}")
        print(f"错误: {result.stderr}")
        return False


def test_cluster_exists(project_id, region, cluster_name):
    """测试集群是否存在"""
    print(f"\n5️⃣  检查 Dataproc 集群: {cluster_name}")
    result = subprocess.run(
        f"gcloud dataproc clusters describe {cluster_name} "
        f"--region={region} --project={project_id}",
        shell=True,
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        print(f"✅ 集群 {cluster_name} 存在且正在运行")
        return True
    else:
        print(f"⚠️  集群 {cluster_name} 不存在或未运行")
        print(f"提示: 使用以下命令创建集群:")
        print(f"\ngcloud dataproc clusters create {cluster_name} \\")
        print(f"  --region={region} \\")
        print(f"  --project={project_id} \\")
        print(f"  --num-workers=2 \\")
        print(f"  --master-machine-type=n1-standard-4 \\")
        print(f"  --worker-machine-type=n1-standard-4")
        return False


def test_data_files():
    """测试本地数据文件"""
    print("\n6️⃣  检查本地数据文件...")
    checks = []
    
    import os
    
    if os.path.exists("data/master_df.csv"):
        print("  ✅ data/master_df.csv 存在")
        checks.append(True)
    else:
        print("  ❌ data/master_df.csv 不存在")
        checks.append(False)
    
    if os.path.exists("ticketmaster_raw/dt=2025-11-21"):
        print("  ✅ ticketmaster_raw/ 数据存在")
        checks.append(True)
    else:
        print("  ❌ ticketmaster_raw/ 数据不存在")
        checks.append(False)
    
    return all(checks)


def main():
    print("="*80)
    print("🔍 Dataproc 配置验证")
    print("="*80)
    
    results = []
    
    # 1. gcloud
    results.append(test_gcloud())
    
    # 2. gsutil
    results.append(test_gsutil())
    
    # 3. 配置文件
    config = load_config()
    if config:
        results.append(True)
        
        # 验证配置值
        print("\n📋 当前配置:")
        for key, value in config.items():
            if not key.startswith("_"):
                print(f"  {key}: {value}")
        
        # 检查是否是默认值
        if config.get("project_id") == "your-gcp-project-id":
            print("\n⚠️  警告: 配置文件使用的是默认值，请修改为实际配置！")
            results.append(False)
        else:
            # 4. GCS 访问
            results.append(test_gcs_access(config['bucket_name']))
            
            # 5. 集群检查
            results.append(test_cluster_exists(
                config['project_id'],
                config['region'],
                config['cluster_name']
            ))
    else:
        results.append(False)
    
    # 6. 本地数据
    results.append(test_data_files())
    
    # 总结
    print("\n" + "="*80)
    if all(results):
        print("✅ 所有检查通过！可以运行 Dataproc 工作流")
        print("\n下一步:")
        print("  python quickstart_integration.py --mode dataproc")
    else:
        print("❌ 部分检查失败，请先解决以上问题")
        print("\n建议:")
        print("  1. 确保 gcloud CLI 已安装并认证")
        print("  2. 编辑 dataproc_config.json 填入实际配置")
        print("  3. 创建 GCS Bucket 和 Dataproc 集群")
        print("  4. 确保本地数据文件存在")
        print("\n详细指南: DATAPROC_SETUP.md")
    print("="*80 + "\n")
    
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ 发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
