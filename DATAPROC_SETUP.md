# Dataproc 快速设置指南

## 🎯 目标

在 Google Cloud Dataproc 上运行 Spark 作业，处理整合后的数据。

---

## 📋 前置要求

1. **Google Cloud Platform 账户**
2. **gcloud CLI 已安装** - https://cloud.google.com/sdk/docs/install
3. **已启用的 API:**
   - Dataproc API
   - Cloud Storage API
   - Compute Engine API

---

## 🚀 快速开始（5 步）

### **Step 1: 创建 GCS Bucket**

```bash
# 设置变量
export PROJECT_ID="your-project-id"
export BUCKET_NAME="your-bucket-name"
export REGION="us-east1"

# 创建 bucket
gsutil mb -p $PROJECT_ID -l $REGION gs://$BUCKET_NAME/

# 验证
gsutil ls gs://$BUCKET_NAME/
```

---

### **Step 2: 创建 Dataproc 集群**

#### **选项 A: 标准集群（推荐用于开发）**
```bash
gcloud dataproc clusters create ticketmaster-cluster \
  --project=$PROJECT_ID \
  --region=$REGION \
  --zone=${REGION}-b \
  --master-machine-type=n1-standard-4 \
  --master-boot-disk-size=100 \
  --num-workers=2 \
  --worker-machine-type=n1-standard-4 \
  --worker-boot-disk-size=100 \
  --image-version=2.1-debian11 \
  --optional-components=JUPYTER \
  --enable-component-gateway
```

**成本预估:** ~$0.50-1.00/小时

#### **选项 B: 最小集群（节省成本）**
```bash
gcloud dataproc clusters create ticketmaster-cluster-mini \
  --project=$PROJECT_ID \
  --region=$REGION \
  --zone=${REGION}-b \
  --single-node \
  --master-machine-type=n1-standard-2 \
  --master-boot-disk-size=50 \
  --image-version=2.1-debian11
```

**成本预估:** ~$0.15-0.30/小时

#### **选项 C: 临时集群（最省钱）**
作业完成后自动删除：
```bash
# 在提交作业时加上 --max-idle 参数
# 见 Step 4
```

---

### **Step 3: 配置项目**

编辑 `dataproc_config.json`:
```json
{
  "project_id": "your-actual-project-id",
  "region": "us-east1",
  "cluster_name": "ticketmaster-cluster",
  "bucket_name": "your-actual-bucket-name",
  "data_path": "ticketmaster_data",
  "output_path": "ticketmaster_output"
}
```

---

### **Step 4: 运行快速开始脚本**

```bash
# Dataproc 模式
python quickstart_integration.py --mode dataproc
```

**脚本会自动：**
1. ✅ 本地整合数据
2. ✅ 上传到 GCS
3. ✅ 提交 ETL 作业
4. ✅ 提交分析作业（可选）
5. ✅ 提交 ML 作业（可选）

---

### **Step 5: 查看结果**

```bash
# 查看输出文件
gsutil ls -r gs://$BUCKET_NAME/ticketmaster_output/

# 下载结果
gsutil cp -r gs://$BUCKET_NAME/ticketmaster_output/ ./output/

# 查看 Parquet 文件
python view_parquet.py output/ticketmaster_output/enriched_parquet/
```

---

## 🛠️ 手动提交作业（高级）

### **ETL 作业**
```bash
gcloud dataproc jobs submit pyspark spark_etl_enriched.py \
  --cluster=ticketmaster-cluster \
  --region=us-east1 \
  --project=$PROJECT_ID \
  -- --input gs://$BUCKET_NAME/ticketmaster_data/enriched_events.csv \
     --output gs://$BUCKET_NAME/ticketmaster_output/enriched_parquet
```

### **分析作业**
```bash
gcloud dataproc jobs submit pyspark spark_analysis_events.py \
  --cluster=ticketmaster-cluster \
  --region=us-east1 \
  --project=$PROJECT_ID \
  -- --input gs://$BUCKET_NAME/ticketmaster_output/enriched_parquet \
     --output gs://$BUCKET_NAME/ticketmaster_output/analytics
```

### **ML 作业（票价预测）**
```bash
gcloud dataproc jobs submit pyspark spark_ml_price_prediction.py \
  --cluster=ticketmaster-cluster \
  --region=us-east1 \
  --project=$PROJECT_ID \
  -- --input gs://$BUCKET_NAME/ticketmaster_output/enriched_parquet \
     --metrics-output gs://$BUCKET_NAME/ticketmaster_output/ml/metrics \
     --model-output gs://$BUCKET_NAME/ticketmaster_output/ml/models/price_predictor \
     --model-type rf
```

---

## 💰 成本优化

### **1. 使用临时集群**
作业完成后自动删除：
```bash
gcloud dataproc jobs submit pyspark spark_etl_enriched.py \
  --cluster=ticketmaster-cluster-temp \
  --region=us-east1 \
  --project=$PROJECT_ID \
  --max-idle=10m \
  -- --input gs://$BUCKET_NAME/...
```

### **2. 使用抢占式 Worker**
```bash
gcloud dataproc clusters create ticketmaster-cluster \
  --num-workers=2 \
  --num-preemptible-workers=2 \
  --preemptible-worker-boot-disk-size=50 \
  ...
```

### **3. 及时删除集群**
```bash
gcloud dataproc clusters delete ticketmaster-cluster \
  --region=us-east1 \
  --project=$PROJECT_ID
```

### **4. 使用自动缩放**
```bash
gcloud dataproc clusters create ticketmaster-cluster \
  --enable-autoscaling \
  --autoscaling-policy=projects/$PROJECT_ID/regions/$REGION/autoscalingPolicies/default \
  ...
```

---

## 📊 监控作业

### **查看作业状态**
```bash
# 列出所有作业
gcloud dataproc jobs list \
  --region=$REGION \
  --project=$PROJECT_ID

# 查看特定作业
gcloud dataproc jobs describe <JOB_ID> \
  --region=$REGION \
  --project=$PROJECT_ID
```

### **Web UI**
1. 访问 GCP Console: https://console.cloud.google.com/dataproc
2. 选择你的集群
3. 点击 "Web Interfaces" 查看 Spark UI

---

## 🔧 故障排查

### **问题 1: 权限错误**
```bash
# 确保服务账户有权限访问 GCS
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member=serviceAccount:$SERVICE_ACCOUNT \
  --role=roles/storage.objectAdmin
```

### **问题 2: 集群创建失败**
```bash
# 检查配额
gcloud compute project-info describe --project=$PROJECT_ID

# 检查 API 是否启用
gcloud services list --enabled --project=$PROJECT_ID
```

### **问题 3: 作业失败**
```bash
# 查看作业日志
gcloud dataproc jobs describe <JOB_ID> \
  --region=$REGION \
  --project=$PROJECT_ID

# 查看 Spark 日志
gsutil cat gs://$BUCKET_NAME/google-cloud-dataproc-metainfo/<CLUSTER-UUID>/jobs/<JOB_ID>/driveroutput.000000000
```

---

## 📚 相关文档

- **Dataproc 文档:** https://cloud.google.com/dataproc/docs
- **定价计算器:** https://cloud.google.com/products/calculator
- **最佳实践:** https://cloud.google.com/dataproc/docs/concepts/iam/iam

---

## ✅ 检查清单

**设置前:**
- [ ] GCP 账户已创建
- [ ] gcloud CLI 已安装并认证
- [ ] 已启用必要的 API
- [ ] 已创建 GCS Bucket

**运行前:**
- [ ] `dataproc_config.json` 已正确配置
- [ ] Dataproc 集群已创建
- [ ] 本地数据已整合 (`data/enriched_events.csv`)

**运行后:**
- [ ] 检查 GCS 输出文件
- [ ] 下载结果到本地
- [ ] 删除临时集群（节省成本）

---

## 💡 快速命令参考

```bash
# 设置环境变量
export PROJECT_ID="your-project-id"
export BUCKET_NAME="your-bucket"
export REGION="us-east1"
export CLUSTER_NAME="ticketmaster-cluster"

# 一键创建集群
gcloud dataproc clusters create $CLUSTER_NAME \
  --region=$REGION --num-workers=2 \
  --master-machine-type=n1-standard-4 \
  --worker-machine-type=n1-standard-4

# 一键运行
python quickstart_integration.py --mode dataproc

# 一键清理
gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION
```

---

**需要帮助？** 查看项目 README 或 EXTERNAL_DATA_WORKFLOW.md
