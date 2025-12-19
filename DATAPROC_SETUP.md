# Google Cloud Dataproc Setup Guide

Complete guide for setting up Google Cloud Dataproc clusters to run Apache Spark ML pipelines for concert ticket price prediction.

---

## 📋 Prerequisites

### 1. GCP Account & Project
- Active Google Cloud Platform account
- Existing project with billing enabled
- Project ID (e.g., `eecs-6893-big-data`)

### 2. Required APIs
Enable the following APIs in your GCP project:
```bash
gcloud services enable dataproc.googleapis.com
gcloud services enable compute.googleapis.com
gcloud services enable storage-api.googleapis.com
gcloud services enable storage-component.googleapis.com
```

### 3. Install Google Cloud SDK
**Windows (PowerShell)**:
```powershell
# Download and install from https://cloud.google.com/sdk/docs/install
# Or use Chocolatey
choco install gcloudsdk

# Initialize
gcloud init
```

**Linux/macOS**:
```bash
curl https://sdk.cloud.google.com | bash
exec -l $SHELL
gcloud init
```

### 4. Set Default Project
```bash
gcloud config set project YOUR_PROJECT_ID
gcloud config set compute/region us-central1
gcloud config set compute/zone us-central1-a
```

---

## 🪣 Create GCS Bucket

### Create Bucket for Data and Scripts
```bash
# Create bucket (must be globally unique name)
gsutil mb -l us-central1 gs://your-bucket-name/

# Verify bucket creation
gsutil ls

# Create folder structure
gsutil mkdir gs://your-bucket-name/data/
gsutil mkdir gs://your-bucket-name/scripts/
gsutil mkdir gs://your-bucket-name/output/
```

### Upload Project Files
```powershell
# Upload data file
gsutil cp data/master_df.csv gs://your-bucket-name/data/

# Upload all training scripts
gsutil cp training/*.py gs://your-bucket-name/scripts/

# Upload configuration file
gsutil cp dataproc_config.json gs://your-bucket-name/
```

---

## 🖥️ Create Dataproc Cluster

### Method 1: GCP Console (Recommended for First-Time Users)

1. **Navigate to Dataproc**:
   - Go to https://console.cloud.google.com/dataproc/
   - Click "CREATE CLUSTER"

2. **Cluster Configuration**:
   - **Name**: `spark-ml-cluster` (or your preferred name)
   - **Region**: `us-central1`
   - **Cluster Type**: `Standard (1 master, N workers)`

3. **Machine Configuration**:
   - **Master Node**:
     - Machine type: `n1-standard-4` (4 vCPUs, 15 GB RAM)
     - Disk size: `50 GB` Standard persistent disk
   
   - **Worker Nodes**:
     - Number of workers: `2`
     - Machine type: `n1-standard-4` (4 vCPUs, 15 GB RAM)
     - Disk size: `50 GB` Standard persistent disk per worker

4. **Optional Components** (Advanced Settings):
   - Enable "Jupyter Notebook" (for interactive data exploration)
   - Enable "Component Gateway" (for accessing UIs)

5. **Initialization Actions** (Optional):
   - None required for basic Spark ML pipeline

6. **Click "CREATE"** - Takes ~2-5 minutes

---

### Method 2: gcloud CLI (Recommended for Automation)

**Basic Cluster** (Minimal Cost):
```bash
gcloud dataproc clusters create spark-ml-cluster \
    --region=us-central1 \
    --zone=us-central1-a \
    --master-machine-type=n1-standard-4 \
    --master-boot-disk-size=50 \
    --num-workers=2 \
    --worker-machine-type=n1-standard-4 \
    --worker-boot-disk-size=50 \
    --image-version=2.0-debian10 \
    --enable-component-gateway \
    --optional-components=JUPYTER
```

**Production Cluster** (Higher Performance):
```bash
gcloud dataproc clusters create spark-ml-production \
    --region=us-central1 \
    --zone=us-central1-a \
    --master-machine-type=n1-standard-8 \
    --master-boot-disk-size=100 \
    --num-workers=4 \
    --worker-machine-type=n1-standard-8 \
    --worker-boot-disk-size=100 \
    --image-version=2.0-debian10 \
    --enable-component-gateway \
    --optional-components=JUPYTER \
    --properties=spark:spark.executor.memory=4g,spark:spark.executor.cores=2
```

**Preemptible Workers Cluster** (Cost Savings 60-90%):
```bash
gcloud dataproc clusters create spark-ml-preemptible \
    --region=us-central1 \
    --zone=us-central1-a \
    --master-machine-type=n1-standard-4 \
    --master-boot-disk-size=50 \
    --num-workers=2 \
    --worker-machine-type=n1-standard-4 \
    --worker-boot-disk-size=50 \
    --num-preemptible-workers=4 \
    --image-version=2.0-debian10
```

---

## 📝 Update dataproc_config.json

Create/edit the configuration file with your cluster details:

```json
{
  "project_id": "eecs-6893-big-data",
  "cluster_name": "spark-ml-cluster",
  "region": "us-central1",
  "bucket_name": "your-bucket-name",
  "input_path": "gs://your-bucket-name/data/master_df.csv",
  "output_base_path": "gs://your-bucket-name/output",
  "scripts": {
    "etl": "gs://your-bucket-name/scripts/spark_etl_master.py",
    "analysis": "gs://your-bucket-name/scripts/spark_analysis_master.py",
    "ml_max": "gs://your-bucket-name/scripts/spark_ml_master_max.py",
    "ml_min": "gs://your-bucket-name/scripts/spark_ml_master_min.py",
    "multi_max": "gs://your-bucket-name/scripts/spark_ml_multi_models_max.py",
    "multi_min": "gs://your-bucket-name/scripts/spark_ml_multi_models_min.py"
  }
}
```

**Upload to GCS**:
```bash
gsutil cp dataproc_config.json gs://your-bucket-name/
```

---

## ▶️ Run Spark Jobs on Dataproc

### Using run_master_pipeline.py (Recommended)
```powershell
# Ensure dataproc_config.json is updated with correct values
python training/run_master_pipeline.py --mode dataproc
```

This will automatically:
1. ✅ Submit ETL job
2. ✅ Wait for completion
3. ✅ Submit Analytics job
4. ✅ Submit ML max price job
5. ✅ Submit Multi-model max price job
6. ✅ Submit ML min price job
7. ✅ Submit Multi-model min price job

---

### Manual Job Submission (Individual Scripts)

#### 1️⃣ ETL Job
```bash
gcloud dataproc jobs submit pyspark \
    gs://your-bucket-name/scripts/spark_etl_master.py \
    --cluster=spark-ml-cluster \
    --region=us-central1 \
    -- \
    --input gs://your-bucket-name/data/master_df.csv \
    --output gs://your-bucket-name/output/master_parquet
```

#### 2️⃣ Analytics Job
```bash
gcloud dataproc jobs submit pyspark \
    gs://your-bucket-name/scripts/spark_analysis_master.py \
    --cluster=spark-ml-cluster \
    --region=us-central1 \
    -- \
    --input gs://your-bucket-name/output/master_parquet \
    --output gs://your-bucket-name/output/analytics
```

#### 3️⃣ ML Max Price Job
```bash
gcloud dataproc jobs submit pyspark \
    gs://your-bucket-name/scripts/spark_ml_master_max.py \
    --cluster=spark-ml-cluster \
    --region=us-central1 \
    -- \
    --input gs://your-bucket-name/output/master_parquet \
    --output gs://your-bucket-name/output/ml_results_max \
    --model-type rf
```

#### 4️⃣ Multi-Model Max Price Job
```bash
gcloud dataproc jobs submit pyspark \
    gs://your-bucket-name/scripts/spark_ml_multi_models_max.py \
    --cluster=spark-ml-cluster \
    --region=us-central1 \
    -- \
    --input gs://your-bucket-name/output/master_parquet \
    --output gs://your-bucket-name/output/ml_multi_models_max
```

---

## 📊 Monitor Job Progress

### GCP Console (Visual Monitoring)
1. Go to https://console.cloud.google.com/dataproc/jobs
2. Select your region (e.g., `us-central1`)
3. View job status:
   - 🟢 **Running**: Job in progress
   - ✅ **Succeeded**: Job completed successfully
   - ❌ **Failed**: Job encountered errors

4. Click job to view:
   - **Driver Output**: Application logs
   - **Yarn Logs**: Detailed Spark logs
   - **Job Details**: Configuration, timing, resources

### CLI Monitoring
```bash
# List all jobs
gcloud dataproc jobs list --region=us-central1

# Get specific job status
gcloud dataproc jobs describe JOB_ID --region=us-central1

# View job output logs
gcloud dataproc jobs wait JOB_ID --region=us-central1
```

### Real-Time Logs
```bash
# Stream job logs in real-time
gcloud dataproc jobs wait JOB_ID --region=us-central1
```

---

## 📥 Download Results from GCS

### Download All Output Files
```powershell
# Download entire output directory
gsutil -m cp -r gs://your-bucket-name/output/ ./

# Download specific folder
gsutil cp -r gs://your-bucket-name/output/analytics/ ./output/

# Download single file
gsutil cp gs://your-bucket-name/output/ml_results_max/metrics/*.csv ./
```

### View Files Directly in GCS
```bash
# List files
gsutil ls -r gs://your-bucket-name/output/

# Preview CSV file (first 20 lines)
gsutil cat gs://your-bucket-name/output/analytics/top_cities/*.csv | head -20

# Count files
gsutil ls gs://your-bucket-name/output/analytics/** | wc -l
```

---

## 🛑 Delete Cluster (Save Costs!)

### IMPORTANT: Delete cluster when not in use to avoid charges!

**GCP Console**:
1. Go to https://console.cloud.google.com/dataproc/
2. Select cluster
3. Click "DELETE"

**CLI**:
```bash
gcloud dataproc clusters delete spark-ml-cluster --region=us-central1
```

**Cost Estimate**:
- Cluster running 24/7: **~$200-400/month**
- On-demand usage (4 hours/week): **~$20-40/month**

---

## 💰 Cost Optimization Strategies

### 1. Use Preemptible Workers
- Save 60-90% on compute costs
- Add `--num-preemptible-workers=4` to cluster creation
- Risk: Workers can be terminated (Spark auto-recovery)

### 2. Auto-Scaling
Enable cluster auto-scaling:
```bash
gcloud dataproc clusters create spark-ml-cluster \
    --enable-component-gateway \
    --autoscaling-policy=policy-name
```

### 3. Scheduled Cluster Deletion
Automatically delete cluster after inactivity:
```bash
gcloud dataproc clusters create spark-ml-cluster \
    --max-idle=30m \
    --max-age=2h
```

### 4. Use Regional Buckets
Keep GCS buckets in same region as Dataproc cluster to avoid egress charges.

### 5. Monitor Costs
- View costs: https://console.cloud.google.com/billing/
- Set budget alerts
- Enable cost recommendations

---

## 🔧 Troubleshooting

### Issue 1: "Permission Denied" Error
**Solution**:
```bash
# Grant Dataproc service account access to GCS
gsutil iam ch serviceAccount:YOUR_PROJECT_NUMBER-compute@developer.gserviceaccount.com:objectAdmin gs://your-bucket-name
```

### Issue 2: "Cluster Creation Failed"
**Common Causes**:
- Insufficient quota (check GCP quotas)
- Region/zone unavailable (try different zone)
- Billing not enabled (enable billing)

**Solution**:
```bash
# Check quotas
gcloud compute project-info describe --project=YOUR_PROJECT_ID

# Request quota increase
https://console.cloud.google.com/iam-admin/quotas
```

### Issue 3: "Job Failed - Out of Memory"
**Solution**: Increase executor memory
```bash
gcloud dataproc jobs submit pyspark script.py \
    --cluster=spark-ml-cluster \
    --region=us-central1 \
    --properties=spark.executor.memory=8g,spark.driver.memory=8g
```

### Issue 4: "Cannot Find Input File"
**Solution**: Verify GCS paths
```bash
# Check if file exists
gsutil ls gs://your-bucket-name/data/master_df.csv

# Upload if missing
gsutil cp data/master_df.csv gs://your-bucket-name/data/
```

### Issue 5: "Cluster Too Slow"
**Solutions**:
1. Increase worker count: `--num-workers=4`
2. Upgrade machine type: `--worker-machine-type=n1-highmem-8`
3. Add preemptible workers for scale: `--num-preemptible-workers=8`

---

## 🎓 Quick Reference

### Essential Commands Cheat Sheet
```bash
# Create cluster
gcloud dataproc clusters create NAME --region=REGION

# Submit job
gcloud dataproc jobs submit pyspark SCRIPT --cluster=NAME --region=REGION

# List jobs
gcloud dataproc jobs list --region=REGION

# Delete cluster
gcloud dataproc clusters delete NAME --region=REGION

# Upload to GCS
gsutil cp LOCAL_FILE gs://BUCKET/PATH

# Download from GCS
gsutil cp gs://BUCKET/PATH LOCAL_PATH

# List GCS files
gsutil ls -r gs://BUCKET/
```

### Default Dataproc Cluster Specs
- **Image**: Debian 10 with Hadoop 3.2, Spark 3.1
- **Python**: 3.8+
- **Installed Libraries**: NumPy, pandas, scikit-learn
- **Spark Submit**: Automatically configured

---

## 📚 Additional Resources

- **Dataproc Documentation**: https://cloud.google.com/dataproc/docs
- **Pricing Calculator**: https://cloud.google.com/products/calculator
- **Best Practices**: https://cloud.google.com/dataproc/docs/concepts/best-practices
- **Spark Configuration**: https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/cluster-properties

---

**Last Updated**: December 18, 2025
