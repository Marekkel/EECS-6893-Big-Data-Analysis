#!/usr/bin/env pwsh
# Deploy Concert Ticket Pricing Project to Google Cloud Platform

param(
    [Parameter(Mandatory=$true)]
    [string]$ProjectId,
    
    [Parameter(Mandatory=$true)]
    [string]$BucketName,
    
    [string]$Region = "us-central1"
)

$ErrorActionPreference = "Stop"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "GCP Deployment Script" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Verify gcloud CLI
Write-Host "[1/8] Checking gcloud CLI..." -ForegroundColor Yellow
try {
    $gcloudVersion = gcloud --version 2>&1 | Select-Object -First 1
    Write-Host "True gcloud CLI found: $gcloudVersion" -ForegroundColor Green
} catch {
    Write-Host "gcloud CLI not found!" -ForegroundColor Red
    Write-Host "Please install: https://cloud.google.com/sdk/docs/install" -ForegroundColor Red
    exit 1
}

# Step 2: Set project
Write-Host ""
Write-Host "[2/8] Setting GCP project..." -ForegroundColor Yellow
gcloud config set project $ProjectId
if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to set project" -ForegroundColor Red
    exit 1
}
Write-Host "True Project set to: $ProjectId" -ForegroundColor Green

# Step 3: Enable required APIs
Write-Host ""
Write-Host "[3/8] Enabling required APIs..." -ForegroundColor Yellow
$apis = @(
    "cloudbuild.googleapis.com",
    "run.googleapis.com",
    "containerregistry.googleapis.com",
    "storage.googleapis.com"
)

foreach ($api in $apis) {
    Write-Host "  Enabling $api..." -ForegroundColor Gray
    gcloud services enable $api --project=$ProjectId
}
Write-Host "True APIs enabled" -ForegroundColor Green

# Step 4: Verify bucket exists
Write-Host ""
Write-Host "[4/8] Checking GCS bucket..." -ForegroundColor Yellow
$bucketExists = gsutil ls -b gs://$BucketName 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "True Bucket found: gs://$BucketName" -ForegroundColor Green
} else {
    Write-Host "Bucket not found: gs://$BucketName" -ForegroundColor Red
    Write-Host "Please create the bucket first or check the name" -ForegroundColor Red
    exit 1
}

# Step 5: Verify model files exist in bucket
Write-Host ""
Write-Host "[5/8] Verifying model files in GCS..." -ForegroundColor Yellow
$modelPaths = @(
    "output/ml_multi_models_max/models/random_forest/",
    "output/ml_multi_models_min/models/random_forest/",
    "output/ml_results_max/avg_encoding_max/",
    "output/ml_results_min/avg_encoding_min/",
    "data/master_df.csv"
)

$allExist = $true
foreach ($path in $modelPaths) {
    $fullPath = "gs://$BucketName/$path"
    $exists = gsutil ls $fullPath 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  True Found: $path" -ForegroundColor Green
    } else {
        Write-Host "  Missing: $path" -ForegroundColor Red
        $allExist = $false
    }
}

if (-not $allExist) {
    Write-Host ""
    Write-Host "Some required files are missing from GCS" -ForegroundColor Red
    Write-Host "Please upload them using: gsutil -m cp -r output/ gs://$BucketName/" -ForegroundColor Yellow
    exit 1
}

Write-Host "True All model files verified" -ForegroundColor Green

# Step 6: Update cloudbuild.yaml with bucket name
Write-Host ""
Write-Host "[6/8] Updating cloudbuild.yaml..." -ForegroundColor Yellow
$cloudBuildPath = "cloudbuild.yaml"
if (Test-Path $cloudBuildPath) {
    (Get-Content $cloudBuildPath) -replace '_GCS_BUCKET: ''your-bucket-name''', "_GCS_BUCKET: '$BucketName'" | Set-Content $cloudBuildPath
    Write-Host "True cloudbuild.yaml updated" -ForegroundColor Green
} else {
    Write-Host "cloudbuild.yaml not found" -ForegroundColor Red
    exit 1
}

# Step 7: Build and deploy backend
Write-Host ""
Write-Host "[7/8] Building and deploying backend to Cloud Run..." -ForegroundColor Yellow
Write-Host "This may take 5-10 minutes..." -ForegroundColor Gray

# Change to project root for build context
Set-Location ..
gcloud builds submit --config=deployment/cloudbuild.yaml --substitutions=_GCS_BUCKET=$BucketName
Set-Location deployment

if ($LASTEXITCODE -ne 0) {
    Write-Host "Deployment failed" -ForegroundColor Red
    exit 1
}

Write-Host "True Backend deployed successfully!" -ForegroundColor Green

# Step 8: Get service URL
Write-Host ""
Write-Host "[8/8] Getting service URL..." -ForegroundColor Yellow
$serviceUrl = gcloud run services describe ticket-pricing-api --region=$Region --format="value(status.url)"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Deployment Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Backend API URL:" -ForegroundColor Yellow
Write-Host "  $serviceUrl" -ForegroundColor White
Write-Host ""
Write-Host "Test your API:" -ForegroundColor Yellow
Write-Host "  curl $serviceUrl/api/health" -ForegroundColor White
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Test the API: curl $serviceUrl/api/predict -X POST -H 'Content-Type: application/json' -d '{...}'" -ForegroundColor Gray
Write-Host "  2. Deploy frontend: Run .\deploy_frontend.ps1" -ForegroundColor Gray
Write-Host "  3. Update frontend config with this URL" -ForegroundColor Gray
Write-Host ""

# Save URL to file for frontend deployment
$serviceUrl | Out-File -FilePath "..\backend\backend_url.txt" -NoNewline
Write-Host "Backend URL saved to backend\backend_url.txt" -ForegroundColor Green
