#!/usr/bin/env pwsh
# Deploy Frontend to Google Cloud Storage

param(
    [Parameter(Mandatory=$true)]
    [string]$ProjectId,
    
    [Parameter(Mandatory=$true)]
    [string]$FrontendBucketName,
    
    [string]$Region = "us-central1"
)

$ErrorActionPreference = "Stop"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Frontend Deployment Script" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Get backend URL
Write-Host "[1/7] Getting backend URL..." -ForegroundColor Yellow
if (Test-Path "..\backend\backend_url.txt") {
    $backendUrl = Get-Content "..\backend\backend_url.txt" -Raw
    Write-Host "True Backend URL: $backendUrl" -ForegroundColor Green
} else {
    Write-Host "Enter backend API URL (from Cloud Run):" -ForegroundColor Yellow
    $backendUrl = Read-Host
}

# Step 2: Update frontend config
Write-Host ""
Write-Host "[2/7] Updating frontend configuration..." -ForegroundColor Yellow
$indexPath = "..\frontend\index.html"

if (Test-Path $indexPath) {
    $content = Get-Content $indexPath -Raw -Encoding UTF8
    
    # Replace API endpoint
    $content = $content -replace 'const API_URL = [^;]+;', "const API_URL = `"$backendUrl/api/predict`";"
    
    $content | Set-Content $indexPath -Encoding UTF8 -NoNewline
    Write-Host "True Frontend configured with backend URL" -ForegroundColor Green
} else {
    Write-Host "website/index.html not found" -ForegroundColor Red
    exit 1
}

# Step 3: Create or use existing bucket
Write-Host ""
Write-Host "[3/7] Setting up frontend bucket..." -ForegroundColor Yellow

$bucketExists = gsutil ls -b gs://$FrontendBucketName 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "Creating new bucket: gs://$FrontendBucketName" -ForegroundColor Gray
    gsutil mb -p $ProjectId -l $Region -b on gs://$FrontendBucketName
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Failed to create bucket" -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "True Using existing bucket: gs://$FrontendBucketName" -ForegroundColor Green
}

# Step 4: Configure bucket for static website hosting
Write-Host ""
Write-Host "[4/7] Configuring bucket for static website hosting..." -ForegroundColor Yellow

# Set default page
gsutil web set -m index.html -e index.html gs://$FrontendBucketName

# Make bucket public
gsutil iam ch allUsers:objectViewer gs://$FrontendBucketName

Write-Host "True Bucket configured for public access" -ForegroundColor Green

# Step 5: Upload website files
Write-Host ""
Write-Host "[5/7] Uploading website files..." -ForegroundColor Yellow

gsutil -m cp -r ../frontend/* gs://$FrontendBucketName/

if ($LASTEXITCODE -ne 0) {
    Write-Host "Upload failed" -ForegroundColor Red
    exit 1
}

Write-Host "True Website files uploaded" -ForegroundColor Green

# Step 6: Set cache control
Write-Host ""
Write-Host "[6/7] Setting cache control..." -ForegroundColor Yellow

gsutil -m setmeta -h "Cache-Control:public, max-age=3600" gs://$FrontendBucketName/*.html
gsutil -m setmeta -h "Cache-Control:public, max-age=86400" gs://$FrontendBucketName/*.css
gsutil -m setmeta -h "Cache-Control:public, max-age=86400" gs://$FrontendBucketName/*.js

Write-Host "True Cache control configured" -ForegroundColor Green

# Step 7: Get website URL
Write-Host ""
Write-Host "[7/7] Getting website URL..." -ForegroundColor Yellow

$websiteUrl = "https://storage.googleapis.com/$FrontendBucketName/index.html"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Frontend Deployment Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Website URL:" -ForegroundColor Yellow
Write-Host "  $websiteUrl" -ForegroundColor White
Write-Host ""
Write-Host "Optional: Set up custom domain" -ForegroundColor Yellow
Write-Host "  1. Create a Load Balancer in GCP Console" -ForegroundColor Gray
Write-Host "  2. Add backend bucket: $FrontendBucketName" -ForegroundColor Gray
Write-Host "  3. Configure your domain DNS" -ForegroundColor Gray
Write-Host "  4. Add SSL certificate" -ForegroundColor Gray
Write-Host ""
Write-Host "Documentation:" -ForegroundColor Yellow
Write-Host "  https://cloud.google.com/storage/docs/hosting-static-website" -ForegroundColor Gray
Write-Host ""

# Save URL to file
@"
Frontend URL: $websiteUrl
Backend URL: $backendUrl
"@ | Out-File -FilePath "..\frontend\deployment_urls.txt"

Write-Host "URLs saved to frontend\deployment_urls.txt" -ForegroundColor Green
