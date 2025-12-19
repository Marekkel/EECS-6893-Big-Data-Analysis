# EECS-6893 Big Data Analysis - Concert Ticket Pricing Project

A comprehensive big data analysis and machine learning pipeline for predicting concert ticket prices using Apache Spark and Google Cloud Platform.

---

## 📋 Table of Contents

- [Project Overview](#project-overview)
- [Key Features](#key-features)
- [Project Structure](#project-structure)
- [Technology Stack](#technology-stack)
- [Quick Start](#quick-start)
- [Documentation](#documentation)
- [License](#license)

---

## 🎯 Project Overview

This project integrates **multi-source concert event data** from Ticketmaster, SeatGeek, StubHub, and Spotify to build a complete big data analytics pipeline. It includes:

- **ETL Processing**: Clean and transform 5,102 concert events from 2017
- **Multi-Dimensional Analytics**: 6 analytical perspectives (geographic, temporal, genre-based)
- **Machine Learning**: Predict Ticketmaster primary market prices (both max and min prices)
- **Model Comparison**: Train and compare 6 regression algorithms
- **Cloud Deployment**: Fully deployable on Google Cloud Platform (Dataproc + Cloud Run + Storage)
- **Web Application**: Interactive price prediction interface

**Data Sources**: Ticketmaster + SeatGeek + StubHub + Spotify  
**Data Volume**: 5,102 concert events from 2017  
**Prediction Target**: Ticketmaster primary market ticket prices (max and min)

---

## ✨ Key Features

### 1. **Distributed ETL Processing** (`training/spark_etl_master.py`)
- Parse complex nested data structures (artist lists, Spotify metrics)
- Type conversion and feature engineering
- Output: Parquet format partitioned by year and month

### 2. **Multi-Dimensional Analytics** (`training/spark_analysis_master.py`)
- **6 Analysis Dimensions**:
  1. Year × Genre trends
  2. Top 50 cities by event volume
  3. Top 100 artists ranked by Spotify popularity
  4. Weekday distribution analysis
  5. Secondary market premium analysis
  6. State-wise price comparison
- Output: 6 CSV reports

### 3. **Single-Model Training** (`training/spark_ml_master_max.py` & `spark_ml_master_min.py`)
- **3 Algorithm Choices**: Random Forest / GBT / Linear Regression
- **Feature Engineering**: Average encoding for categorical variables
- **Dual Prediction**: Separate models for max price and min price
- Output: Predictions, metrics (RMSE/MAE/R²), feature importance

### 4. **Multi-Model Comparison** (`training/spark_ml_multi_models_max.py` & `spark_ml_multi_models_min.py`) ⭐
- **6 Regression Models**:
  - Linear Regression
  - Lasso Regression (L1 regularization)
  - Elastic Net (L1+L2 regularization)
  - Decision Tree
  - Random Forest
  - Gradient Boosted Trees (GBT)
- **Automatic Comparison**: RMSE, MAE, R² metrics
- Output: Model comparison report, predictions, feature importance

### 5. **One-Click Pipeline** (`training/run_master_pipeline.py`)
- **Local Mode**: Run on-premise with Spark
- **Dataproc Mode**: Run on Google Cloud Dataproc
- **6-Step Pipeline**: ETL → Analytics → Single ML (MAX) → Multi ML (MAX) → Single ML (MIN) → Multi ML (MIN)

### 6. **Production Deployment**
- **Backend API** (`backend/`): FastAPI + Spark ML for real-time predictions
- **Frontend** (`frontend/`): Interactive web interface
- **Cloud Deployment** (`deployment/`): Docker + Cloud Run + Cloud Storage
- **Fully Automated**: PowerShell scripts for one-click deployment

---

## 📁 Project Structure

```
EECS-6893-Big-Data-Analysis/
├── README.md                     # Main documentation
├── QUICKSTART.md                 # Quick start guide
├── OUTPUT_GUIDE.md               # Output files reference
├── DATAPROC_SETUP.md             # Dataproc setup guide
├── GCP_DEPLOYMENT_GUIDE.md       # Complete GCP deployment guide
├── dataproc_config.json          # Dataproc configuration
│
├── data/
│   └── master_df.csv             # Main dataset (5,102 events)
│
├── training/                     # Training scripts directory
│   ├── run_master_pipeline.py    # Pipeline orchestration script
│   ├── spark_etl_master.py       # ETL processing
│   ├── spark_analysis_master.py  # Multi-dimensional analytics
│   ├── spark_ml_master_max.py    # Single model (max price)
│   ├── spark_ml_master_min.py    # Single model (min price)
│   ├── spark_ml_multi_models_max.py  # Multi-model comparison (max)
│   └── spark_ml_multi_models_min.py  # Multi-model comparison (min)
│
├── backend/                      # Backend API
│   ├── app_gcp.py                # FastAPI application
│   ├── predictor_gcp.py          # Spark ML predictor
│   ├── requirements.txt          # Python dependencies
│   └── README.md                 # Backend documentation
│
├── frontend/                     # Frontend web interface
│   └── index.html                # Main HTML file
│
├── deployment/                   # Deployment scripts
│   ├── Dockerfile                # Docker configuration
│   ├── cloudbuild.yaml           # Cloud Build configuration
│   ├── deploy_to_gcp.ps1         # Backend deployment script
│   └── deploy_frontend.ps1       # Frontend deployment script
│
├── tools/
│   └── view_parquet.py           # Parquet file viewer utility
│
├── price_avg/                    # Pre-computed average prices
├── result/                       # Jupyter notebooks for analysis
└── output/                       # Generated output
    ├── master_parquet/           # Cleaned data (Parquet)
    ├── analytics/                # Analytics results (6 CSV files)
    ├── ml_results_max/           # Single model results (max price)
    ├── ml_results_min/           # Single model results (min price)
    ├── ml_multi_models_max/      # Multi-model results (max price)
    └── ml_multi_models_min/      # Multi-model results (min price)
```

---

## 🛠️ Technology Stack

### Big Data & Machine Learning
- **Apache Spark 3.x** - Distributed data processing
- **PySpark** - Python API for Spark
- **Spark MLlib** - Machine learning library (6 regression algorithms)

### Backend & API
- **FastAPI** - Modern web framework
- **Uvicorn** - ASGI server
- **Spark ML** - Real-time model inference

### Cloud Infrastructure
- **Google Cloud Dataproc** - Managed Spark clusters
- **Google Cloud Run** - Serverless container platform
- **Google Cloud Storage** - Object storage for models and data

### Frontend
- **HTML/CSS/JavaScript** - Interactive web interface
- **Chart.js** - Data visualization

### Development Tools
- **Docker** - Containerization
- **PowerShell** - Deployment automation
- **Python 3.10+** - Primary programming language

---

## 🚀 Quick Start

### Prerequisites

1. **For Training (Local)**:
   - Python 3.7+ 
   - Apache Spark 3.x
   - Java 8 or 11

2. **For Training (Cloud)**:
   - Google Cloud SDK
   - GCP account with Dataproc enabled
   - GCS bucket

3. **For Deployment**:
   - Docker (optional)
   - GCP account with Cloud Run enabled

### Option 1: Local Training

```powershell
# Navigate to training directory
cd training

# Run complete pipeline (ETL → Analytics → ML)
python run_master_pipeline.py --mode local

# Or run from project root
python training/run_master_pipeline.py --mode local
```

**Expected Output**: `output/` directory with all results

**Estimated Time**: 10-20 minutes (depending on machine specs)

### Option 2: Cloud Training (Dataproc)

```powershell
# 1. Configure GCP settings
# Edit dataproc_config.json with your project details

# 2. Run on Dataproc
cd training
python run_master_pipeline.py --mode dataproc
```

**Expected Output**: Results in GCS bucket `gs://your-bucket/output/`

**See**: [DATAPROC_SETUP.md](DATAPROC_SETUP.md) for detailed setup

### Option 3: Deploy Web Application

```powershell
# 1. Deploy backend API to Cloud Run
cd deployment
.\deploy_to_gcp.ps1 -ProjectId "your-project-id" -BucketName "your-bucket"

# 2. Deploy frontend to Cloud Storage
.\deploy_frontend.ps1 -ProjectId "your-project-id" -FrontendBucketName "your-frontend-bucket"
```

**Result**: Fully functional web application accessible via public URL

**See**: [GCP_DEPLOYMENT_GUIDE.md](GCP_DEPLOYMENT_GUIDE.md) for complete guide

---

## 📚 Documentation

### Core Documentation
- **[README.md](README.md)** - This file (project overview)
- **[QUICKSTART.md](QUICKSTART.md)** - Step-by-step quick start guide
- **[OUTPUT_GUIDE.md](OUTPUT_GUIDE.md)** - Detailed explanation of all output files

### Setup Guides
- **[DATAPROC_SETUP.md](DATAPROC_SETUP.md)** - Configure and use Google Cloud Dataproc
- **[GCP_DEPLOYMENT_GUIDE.md](GCP_DEPLOYMENT_GUIDE.md)** - Deploy backend API and frontend

### Component Documentation
- **[backend/README.md](backend/README.md)** - Backend API documentation
- **[backend/QUICKSTART.md](backend/QUICKSTART.md)** - Backend quick reference

---

## 📊 Key Results

### Dataset Statistics
- **Total Events**: 5,102 concerts
- **Time Period**: 2017
- **Data Sources**: 4 platforms (Ticketmaster, SeatGeek, StubHub, Spotify)
- **Features**: 30+ features after engineering

### Model Performance
| Model | RMSE (Max) | MAE (Max) | R² (Max) | RMSE (Min) | MAE (Min) | R² (Min) |
|-------|------------|-----------|----------|------------|-----------|----------|
| Random Forest | 93.7 | 42.1 | 0.617 | 72.3 | 35.8 | 0.608 |
| GBT | 95.2 | 43.5 | 0.605 | 74.1 | 36.9 | 0.595 |
| Linear Regression | 105.6 | 48.2 | 0.521 | 82.4 | 41.3 | 0.510 |

**Best Model**: Random Forest (for both max and min price prediction)

### Output Files
- **6 Analytics Reports** (CSV format)
- **2 Trained Models** (Random Forest for max and min prices)
- **12 Model Comparisons** (6 models × 2 targets)
- **Feature Importance Rankings** for tree-based models

---

## 🎯 Use Cases

1. **Concert Organizers**: Optimize ticket pricing strategies
2. **Music Industry Analysis**: Understand market trends and artist popularity
3. **Data Science Education**: Complete big data pipeline example
4. **Research**: Study secondary market premium and pricing dynamics

---

## 🤝 Contributing

This is an academic project for EECS-6893 Big Data Analysis. Contributions and suggestions are welcome!

---

## 📄 License

MIT License - Feel free to use this project for educational purposes.

---

## 👥 Authors

EECS-6893 Big Data Analysis - Final Project Team

**Institution**: Columbia University  
**Course**: EECS-6893 Big Data Analysis  
**Year**: 2024-2025

---

## 🔗 Related Links

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Google Cloud Dataproc](https://cloud.google.com/dataproc)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Spark MLlib Guide](https://spark.apache.org/docs/latest/ml-guide.html)

---

**Last Updated**: December 18, 2025
