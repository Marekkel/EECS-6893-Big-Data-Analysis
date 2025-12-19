# 🚀 Complete GCP Deployment Guide - Concert Ticket Pricing Project

Deploy the entire project to Google Cloud Platform, including backend API and frontend website.

---

## 📋 Table of Contents

1. [Deployment Architecture](#deployment-architecture)
2. [Prerequisites](#prerequisites)
3. [Quick Deployment](#quick-deployment)
4. [Detailed Steps](#detailed-steps)
5. [Custom Domain Configuration](#custom-domain-configuration)
6. [Monitoring and Maintenance](#monitoring-and-maintenance)
7. [Cost Estimation](#cost-estimation)
8. [Troubleshooting](#troubleshooting)

---

## 🏗️ Deployment Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      User Browser                            │
│                  (Access via Public URL)                     │
└────────────────┬────────────────────────────────────────────┘
                 │ HTTPS
                 │
    ┌────────────▼───────────────┐
    │  Frontend (Static Website)  │
    │  • Google Cloud Storage     │
    │  • HTML/CSS/JavaScript      │
    │  • index.html               │
    └────────────┬───────────────┘
                 │ HTTPS/API Calls
                 │
    ┌────────────▼───────────────┐
    │   Backend API (Cloud Run)   │
    │  • FastAPI + PySpark        │
    │  • Container (Docker)       │
    │  • Auto-scaling             │
    └────────────┬───────────────┘
                 │ Reads models/data
                 │
    ┌────────────▼───────────────┐
    │  Google Cloud Storage       │
    │  • ML models (output/)      │
    │  • Training data (data/)    │
    │  • Encodings (CSV files)    │
    └─────────────────────────────┘
```

### Component Description

1. **Frontend (Cloud Storage)**
   - Static website hosting
   - Global CDN acceleration
   - Extremely low cost (~$1/month)

2. **Backend (Cloud Run)**
   - Containerized deployment (Docker)
   - Auto-scaling (0-10 instances)
   - Pay-per-use pricing
   - Complete Spark ML environment

3. **Data Storage (Cloud Storage)**
   - Store trained models
   - Store encoding mappings and data
   - High availability

---

## 📦 Prerequisites

### 1. Completed Tasks
- ✅ Trained RandomForest models on GCP Dataproc
- ✅ Model files uploaded to GCS bucket
- ✅ Complete project code locally available

### 2. Required Tools

```powershell
# Check if already installed
gcloud --version
python --version
docker --version
```

If not installed:
- **Google Cloud SDK**: https://cloud.google.com/sdk/docs/install
- **Python 3.10+**: https://www.python.org/downloads/
- **Docker Desktop** (optional): https://www.docker.com/products/docker-desktop

### 3. Required GCP Permissions

Ensure your GCP account has the following permissions:
- Cloud Run Admin
- Cloud Build Editor
- Storage Admin
- Service Account User

### 4. GCS Bucket Structure

Ensure your bucket contains the following files:

```
gs://your-bucket-name/
├── output/
│   ├── ml_multi_models_max/
│   │   └── models/
│   │       └── random_forest/
│   │           ├── metadata/
│   │           ├── data/
│   │           └── treesMetadata/
│   ├── ml_multi_models_min/
│   │   └── models/
│   │       └── random_forest/
│   ├── ml_results_max/
│   │   └── avg_encoding_max/
│   │       ├── city_avg/
│   │       ├── state_avg/
│   │       ├── genre_avg/
│   │       ├── subgenre_avg/
│   │       └── global_avg/
│   └── ml_results_min/
│       └── avg_encoding_min/
│           ├── city_avg/
│           ├── state_avg/
│           ├── genre_avg/
│           ├── subgenre_avg/
│           └── global_avg/
└── data/
    └── master_df.csv
```

---

## ⚡ Quick Deployment

### Method 1: One-Click Deployment Scripts (Recommended)

```powershell
# 1. Deploy Backend API
cd deployment
.\deploy_to_gcp.ps1 -ProjectId "your-project-id" -BucketName "your-bucket-name"

# 2. Deploy Frontend Website
.\deploy_frontend.ps1 -ProjectId "your-project-id" -FrontendBucketName "your-frontend-bucket"
```

**Done!** 🎉 Your website is now live on the public internet.

### Method 2: Manual Deployment

If scripts have issues, follow the [Detailed Steps](#detailed-steps) for manual operation.

---

## 📝 Detailed Steps

### Part 1: Deploy Backend API

#### Step 1: Set Up GCP Project

```powershell
# Login to GCP
gcloud auth login

# Set project
gcloud config set project YOUR_PROJECT_ID

# View current configuration
gcloud config list
```

#### Step 2: Enable Required APIs

```powershell
# Enable Cloud Run
gcloud services enable run.googleapis.com

# Enable Cloud Build
gcloud services enable cloudbuild.googleapis.com

# Enable Container Registry
gcloud services enable containerregistry.googleapis.com

# Enable Storage
gcloud services enable storage.googleapis.com
```

#### Step 3: Verify Model Files

```powershell
# Check model files in bucket
gsutil ls gs://your-bucket-name/output/ml_multi_models_max/models/random_forest/

# Check data files
gsutil ls gs://your-bucket-name/data/master_df.csv

# If files missing, upload from local
gsutil -m cp -r output/ gs://your-bucket-name/
gsutil -m cp -r data/ gs://your-bucket-name/
```

#### Step 4: Update Configuration File

Edit `deployment/cloudbuild.yaml`, replace `your-bucket-name` with your actual bucket name:

```yaml
substitutions:
  _GCS_BUCKET: 'your-actual-bucket-name'  # Change this
```

#### Step 5: Build and Deploy

```powershell
# Submit build task (takes 5-10 minutes)
cd deployment
gcloud builds submit --config=cloudbuild.yaml

# After build completes, check service status
gcloud run services list
```

#### Step 6: Get Backend URL

```powershell
# Get service URL
gcloud run services describe ticket-pricing-api --region=us-central1 --format="value(status.url)"

# Example output: https://ticket-pricing-api-xxxxx-uc.a.run.app
```

#### Step 7: Test Backend API

```powershell
# Test health check
curl https://ticket-pricing-api-xxxxx-uc.a.run.app/api/health

# Test prediction endpoint
curl -X POST https://ticket-pricing-api-xxxxx-uc.a.run.app/api/predict `
  -H "Content-Type: application/json" `
  -d '{
    "artist": "Taylor Swift",
    "city": "New York",
    "state": "NY",
    "date": "2024-06-15",
    "genre": "Pop"
  }'
```

Expected response:
```json
{
  "prediction": {
    "max_price": 234.67,
    "min_price": 89.50,
    "price_range": 145.17,
    "confidence": "high"
  },
  "input_features": {...},
  "model_info": {
    "model_type": "RandomForest",
    "framework": "Spark MLlib",
    "source": "Google Cloud Storage"
  }
}
```

---

### Part 2: Deploy Frontend Website

#### Step 1: Create Frontend Bucket

```powershell
# Create bucket (bucket name must be globally unique)
gsutil mb -p YOUR_PROJECT_ID -l us-central1 -b on gs://your-frontend-bucket-name

# Example:
# gsutil mb -p my-project -l us-central1 -b on gs://ticket-pricing-frontend
```

#### Step 2: Configure Static Website Hosting

```powershell
# Set main page and error page
gsutil web set -m index.html -e index.html gs://your-frontend-bucket-name

# Set public access
gsutil iam ch allUsers:objectViewer gs://your-frontend-bucket-name
```

#### Step 3: Update Frontend Configuration

Edit `frontend/index.html`, find the API configuration section:

```javascript
// Find this line
const API_BASE_URL = 'http://localhost:8000';

// Change to your Cloud Run URL
const API_BASE_URL = 'https://ticket-pricing-api-xxxxx-uc.a.run.app';
```

#### Step 4: Upload Website Files

```powershell
# Upload all files
gsutil -m cp -r frontend/* gs://your-frontend-bucket-name/

# Set cache policy
gsutil -m setmeta -h "Cache-Control:public, max-age=3600" gs://your-frontend-bucket-name/*.html
gsutil -m setmeta -h "Cache-Control:public, max-age=86400" gs://your-frontend-bucket-name/*.css
gsutil -m setmeta -h "Cache-Control:public, max-age=86400" gs://your-frontend-bucket-name/*.js
```

#### Step 5: Get Website URL

```powershell
# Your website URL
echo "https://storage.googleapis.com/your-frontend-bucket-name/index.html"
```

**Visit this URL and your website is live!** 🎉

---

## 🌐 Custom Domain Configuration (Optional)

If you want to use your own domain (e.g., `ticket-pricing.example.com`):

### Method 1: Using Cloud Load Balancer

1. **Create Load Balancer**
   ```powershell
   # Easier to do in GCP Console
   # Visit: https://console.cloud.google.com/net-services/loadbalancing
   ```

2. **Add Backend Bucket**
   - Backend configuration → Create a backend bucket
   - Select your frontend bucket
   - Enable Cloud CDN

3. **Configure Domain**
   - Frontend configuration → Add your domain
   - Google automatically configures SSL certificate

4. **Update DNS Records**
   ```
   Type: A
   Name: ticket-pricing (or @)
   Value: [Load Balancer IP]
   ```

### Method 2: Using Firebase Hosting (Simpler)

```powershell
# Install Firebase CLI
npm install -g firebase-tools

# Login
firebase login

# Initialize project
firebase init hosting

# Deploy
firebase deploy --only hosting
```

Configure custom domain:
```powershell
firebase hosting:channel:deploy production
```

---

## 📊 Monitoring and Maintenance

### View Logs

```powershell
# View Cloud Run logs
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=ticket-pricing-api" --limit 50

# Tail logs in real-time
gcloud logging tail "resource.type=cloud_run_revision AND resource.labels.service_name=ticket-pricing-api"
```

### Monitor Metrics

View in GCP Console:
- **Cloud Run Metrics**: https://console.cloud.google.com/run
  - Request count
  - Response time
  - Error rate
  - Instance count

- **Storage Metrics**: https://console.cloud.google.com/storage
  - Storage usage
  - Request count
  - Bandwidth usage

### Set Up Alerts

```powershell
# Create alert policy (notify when error rate > 5%)
gcloud alpha monitoring policies create \
  --notification-channels=YOUR_CHANNEL_ID \
  --display-name="High Error Rate" \
  --condition-threshold-value=5 \
  --condition-threshold-duration=60s
```

---

## 💰 Cost Estimation

Based on moderate usage (100-500 prediction requests per day):

### Backend (Cloud Run)
- **Instance runtime**: $0.00002400 per vCPU-second
- **Memory usage**: $0.00000250 per GiB-second
- **Request fee**: $0.40 per million requests
- **Estimated**: ~$5-15/month

### Frontend (Cloud Storage)
- **Storage**: $0.020 per GB/month
- **Network egress**: $0.12 per GB (first 1GB free)
- **Requests**: $0.004 per 10,000 Class A operations
- **Estimated**: ~$0.50-2/month

### Data Storage (Model Files)
- **Storage**: ~$0.50/month (~25GB models + data)

**Total**: ~$6-20/month

### Cost Saving Tips

1. **Use Cloud Run minimum instances = 0** (already configured)
   - No charges when idle
   - Cold start time: 30-60 seconds

2. **Enable Cloud CDN**
   - Reduce Storage requests
   - Speed up global access

3. **Set Reasonable Cache Policies**
   - HTML: 1 hour
   - CSS/JS: 24 hours

4. **Monitor and Optimize**
   - Regularly check Cost Reports
   - Delete unnecessary resources

---

## 🔧 Troubleshooting

### Issue 1: Cloud Build Failed

**Error**: `ERROR: failed to build: executing lifecycle`

**Solution**:
```powershell
# Check Dockerfile syntax
docker build -t test .

# View complete build logs
gcloud builds list --limit=1
gcloud builds log [BUILD_ID]
```

### Issue 2: Backend Startup Timeout

**Error**: `Container failed to start. Failed to start and then listen on the port defined by the PORT environment variable.`

**Solution**:
1. Increase timeout and memory:
   ```powershell
   gcloud run services update ticket-pricing-api \
     --timeout=300 \
     --memory=4Gi \
     --region=us-central1
   ```

2. Check logs:
   ```powershell
   gcloud logging read "resource.type=cloud_run_revision" --limit=50
   ```

### Issue 3: Model Loading Failed

**Error**: `FileNotFoundError: No CSV file found in gs://...`

**Solution**:
```powershell
# Verify file exists
gsutil ls -r gs://your-bucket-name/output/ml_results_max/avg_encoding_max/

# Check permissions
gsutil iam get gs://your-bucket-name

# Grant Cloud Run service account access
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:YOUR_SERVICE_ACCOUNT@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer"
```

### Issue 4: CORS Error

**Error**: `Access to fetch at 'https://...' from origin 'https://...' has been blocked by CORS policy`

**Solution**:
Backend already configured with `allow_origins=["*"]`, but if still having issues:

1. Check CORS configuration in `backend/app_gcp.py`
2. Ensure frontend uses HTTPS (not HTTP)
3. Clear browser cache

### Issue 5: Slow Predictions

**Cause**: Spark initialization takes time

**Optimization**:
1. Increase minimum instances (avoid cold starts):
   ```powershell
   gcloud run services update ticket-pricing-api \
     --min-instances=1 \
     --region=us-central1
   ```
   Note: Will increase costs (~$30/month)

2. Use Cloud Run Jobs for warmup (scheduled requests)

### Issue 6: Website Inaccessible

**Checklist**:
```powershell
# 1. Check if bucket is public
gsutil iam get gs://your-frontend-bucket-name

# 2. Check if files are uploaded
gsutil ls gs://your-frontend-bucket-name/

# 3. Check web configuration
gsutil web get gs://your-frontend-bucket-name

# 4. Test direct access
curl https://storage.googleapis.com/your-frontend-bucket-name/index.html
```

---

## 🚀 Advanced Configuration

### 1. Enable Cloud CDN (Speed Up Global Access)

Requires Load Balancer, see [Custom Domain Configuration](#custom-domain-configuration)

### 2. Configure CI/CD Auto Deployment

Create GitHub Actions workflow (`.github/workflows/deploy.yml`):

```yaml
name: Deploy to GCP

on:
  push:
    branches: [ main ]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - uses: google-github-actions/setup-gcloud@v0
        with:
          service_account_key: ${{ secrets.GCP_SA_KEY }}
          project_id: ${{ secrets.GCP_PROJECT_ID }}
      
      - name: Deploy Backend
        run: |
          cd deployment
          gcloud builds submit --config=cloudbuild.yaml
      
      - name: Deploy Frontend
        run: |
          gsutil -m cp -r frontend/* gs://${{ secrets.FRONTEND_BUCKET }}/
```

### 3. Set Up Custom Service Account

```powershell
# Create service account
gcloud iam service-accounts create ticket-pricing-sa \
  --display-name="Ticket Pricing Service Account"

# Grant permissions
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:ticket-pricing-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer"

# Deploy with service account
gcloud run deploy ticket-pricing-api \
  --service-account=ticket-pricing-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

---

## 📚 Related Resources

- **Cloud Run Documentation**: https://cloud.google.com/run/docs
- **Cloud Storage Static Websites**: https://cloud.google.com/storage/docs/hosting-static-website
- **Cloud Build Documentation**: https://cloud.google.com/build/docs
- **Pricing Calculator**: https://cloud.google.com/products/calculator

---

## 🎯 Deployment Checklist

Backend Deployment:
- [ ] Required GCP APIs enabled
- [ ] Model files uploaded to GCS bucket
- [ ] Bucket name updated in cloudbuild.yaml
- [ ] Cloud Build completed successfully
- [ ] Cloud Run service running normally
- [ ] `/api/health` returns "healthy"
- [ ] `/api/predict` makes predictions successfully

Frontend Deployment:
- [ ] Frontend bucket created and configured as public
- [ ] API_BASE_URL updated in index.html
- [ ] Website files uploaded to bucket
- [ ] Website accessible via browser
- [ ] Website can call backend API
- [ ] Prediction functionality works correctly

---

## 🆘 Need Help?

If you encounter issues:

1. **View logs**: `gcloud logging read ...`
2. **Check service status**: https://console.cloud.google.com/run
3. **Verify permissions**: Ensure service account has sufficient permissions
4. **Test locally**: Test in local Docker first
5. **Contact support**: GCP Support or Stack Overflow

---

**Happy Deploying!** 🎉

**Last Updated**: December 18, 2025  
**Version**: 1.0.0
