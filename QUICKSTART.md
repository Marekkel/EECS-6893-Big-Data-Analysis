# 🚀 Quick Start Guide - Concert Ticket Pricing Project

Get up and running in 5 minutes!

---

## 📋 Project Overview

A complete big data analytics and machine learning project based on **5,102 concert events from 2017**.

**Data Sources**: Ticketmaster + SeatGeek + StubHub + Spotify  
**Goal**: Predict Ticketmaster primary market ticket prices (both max and min)

---

## 🎯 Core Capabilities

### 1️⃣ **ETL Data Cleaning**
- **Script**: `training/spark_etl_master.py`
- **Function**: Parse CSV, type conversion, feature engineering
- **Output**: Parquet format (partitioned by year/month)

### 2️⃣ **Statistical Analysis** (6 Dimensions)
- **Script**: `training/spark_analysis_master.py`
- **Analysis Content**:
  1. Year × Genre trends
  2. Top 50 cities
  3. Top 100 artists (ranked by Spotify)
  4. Weekday distribution
  5. Secondary market premium analysis
  6. State-wise price rankings

### 3️⃣ **Single Model ML** (RandomForest)
- **Scripts**: `training/spark_ml_master_max.py` & `spark_ml_master_min.py`
- **Models**: Random Forest (optional: GBT, Linear Regression)
- **Output**: Predictions + Metrics + Feature Importance

### 4️⃣ **Multi-Model Comparison** ⭐ NEW
- **Scripts**: `training/spark_ml_multi_models_max.py` & `spark_ml_multi_models_min.py`
- **6 Algorithms Compared**:
  - Linear Regression
  - Lasso Regression (L1 regularization)
  - Elastic Net (L1+L2 regularization)
  - Decision Tree
  - Random Forest
  - Gradient Boosted Trees (GBT)
- **Output**: 
  - 6 trained models
  - Prediction samples for each model
  - Feature importance for tree models
  - Model performance comparison table (RMSE, MAE, R²)

---

## 🏃 How to Run

### Local Execution (Requires Spark Environment)
```powershell
# Run from project root
python training/run_master_pipeline.py --mode local

# Or navigate to training directory first
cd training
python run_master_pipeline.py --mode local
```

**Output Location**: `output/` folder (relative to project root)

### Dataproc Execution (Cloud)
```powershell
# Run from project root
python training/run_master_pipeline.py --mode dataproc

# Or navigate to training directory first
cd training
python run_master_pipeline.py --mode dataproc
```

**Output Location**: `gs://your-bucket/output/`

---

## 📊 Output File Structure

```
output/
├── master_parquet/           # Cleaned data (Parquet)
├── analytics/                # 6 statistical analysis CSV files
│   ├── events_per_year_genre/
│   ├── top_cities/
│   ├── top_artists/
│   ├── events_per_weekday/
│   ├── secondary_market_by_genre/
│   └── price_by_state/
├── ml_results_max/           # Single model ML results (max price)
│   ├── predictions/
│   ├── metrics/
│   └── feature_importance/
├── ml_results_min/           # Single model ML results (min price)
├── ml_multi_models_max/      # ⭐ Multi-model comparison (max price)
│   ├── models/               # 6 model files
│   ├── predictions_sample/   # Prediction samples for each model
│   ├── feature_importance/   # Tree model feature importance
│   └── metrics_comparison_csv/ # Model performance comparison ⭐
└── ml_multi_models_min/      # Multi-model comparison (min price)
```

---

## 🔑 Key Files Explained

### 📈 Statistical Analysis Results
| File | Content | Usage |
|------|---------|-------|
| `top_cities.csv` | City event rankings | Heatmap visualization |
| `top_artists.csv` | Artist leaderboard | Spotify followers vs. ticket price analysis |
| `events_per_year_genre.csv` | Time trends | Line chart: genre trends over time |
| `secondary_market_by_genre.csv` | Premium rates | Bar chart: secondary market premium comparison |

### 🤖 Machine Learning Results
| File | Content | Usage |
|------|---------|-------|
| **`metrics_comparison_csv/*.csv`** ⭐ | **6 model performance comparison** | **Find the best model** |
| `predictions_sample/*/*.csv` | Prediction samples for each model | Scatter plot: actual vs. predicted |
| `feature_importance/*/*.csv` | Feature importance rankings | Horizontal bar chart: key factors |

---

## 📊 Model Performance Comparison (Example)

View comparison results after running:
```powershell
# View max price model comparison
Get-Content output/ml_multi_models_max/metrics_comparison_csv/part-00000-*.csv

# View min price model comparison
Get-Content output/ml_multi_models_min/metrics_comparison_csv/part-00000-*.csv
```

**Expected Output**:
```csv
model,rmse,mae,r2
random_forest,15.32,11.85,0.8245    ← Usually the best
gbt,16.18,12.30,0.8102
elastic_net,18.45,14.20,0.7856
lasso_regression,18.78,14.55,0.7801
linear_regression,19.20,15.10,0.7698
decision_tree,21.50,16.80,0.7320
```

**Interpretation**:
- **Lowest RMSE** = Random Forest (prediction error $15.32)
- **Highest R²** = Random Forest (fit 82.45%)
- **Linear vs. Tree Models**: Tree models have higher accuracy

---

## 🎨 Visualization Suggestions

### 1. Model Comparison Bar Chart
- X-axis: 6 models
- Y-axis: RMSE / MAE / R²
- Instantly identify the best model

### 2. Prediction Accuracy Scatter Plot
- X-axis: actual_price
- Y-axis: predicted_price
- Diagonal line = perfect prediction
- Create one plot per model to compare deviation

### 3. Feature Importance Horizontal Bar Chart
- Compare 3 tree models (DT, RF, GBT)
- Identify consistently important features
- Business insight: key pricing factors

### 4. Geographic Heatmap
- Use `top_cities.csv`
- US map with event density markers

### 5. Time Trend Line Chart
- Use `events_per_year_genre.csv`
- Event volume changes by genre over time

---

## 🆚 Single Model vs. Multi-Model

| Feature | Single Model (spark_ml_master_*.py) | Multi-Model (spark_ml_multi_models_*.py) |
|---------|-------------------------------------|------------------------------------------|
| **Number of Models** | 1 (choice of algorithm) | 6 (comprehensive comparison) |
| **Training Time** | Fast (~5 minutes) | Slower (~20 minutes) |
| **Output** | Single result | Performance comparison table ⭐ |
| **Use Case** | Quick validation | Final report/paper |
| **Feature Importance** | Yes (if RF/GBT) | 3 tree models compared |
| **Recommended For** | Development phase | Project delivery phase |

---

## 💡 Project Highlights

### Team Contribution Integration ✅
- Original code: Tailored for old Ticketmaster API structure
- After integration: Perfect fit for `master_df.csv` columns
- New features: 6 models auto-trained + performance comparison

### Technical Advantages
1. **Big Data Processing**: Spark distributed computing
2. **Multi-Source Data Fusion**: TM + SG + SH + Spotify
3. **Complete ML Pipeline**: Data cleaning → Feature engineering → Model training → Evaluation
4. **Cloud Scalable**: Dataproc deployment support

### Business Value
- **Pricing Strategy**: Predict secondary market prices to guide original pricing
- **Market Insights**: Identify high-value cities and artists
- **Premium Analysis**: Secondary market profit margins

---

## 🐛 Common Issues

### Q1: What environment is needed for local execution?
**A**: Apache Spark 3.x + Python 3.7+

### Q2: What needs to be modified for Dataproc execution?
**A**: Edit `dataproc_config.json` with your GCP project information

### Q3: What if multi-model training is too slow?
**A**: 
- Local mode: Run only single model (faster)
- Dataproc: Increase cluster nodes

### Q4: How to view the best model?
**A**: 
```powershell
# View max price prediction model comparison
Get-Content output/ml_multi_models_max/metrics_comparison_csv/*.csv | Sort-Object

# View min price prediction model comparison
Get-Content output/ml_multi_models_min/metrics_comparison_csv/*.csv | Sort-Object
```
The one with the smallest RMSE is the best model

### Q5: What can model files be used for?
**A**: Can be loaded for price prediction of new events (requires same features)

---

## 📚 Detailed Documentation

- **Main Documentation**: [README.md](README.md)
- **Output File Reference**: [OUTPUT_GUIDE.md](OUTPUT_GUIDE.md)
- **Dataproc Deployment**: [DATAPROC_SETUP.md](DATAPROC_SETUP.md)
- **Complete GCP Deployment**: [GCP_DEPLOYMENT_GUIDE.md](GCP_DEPLOYMENT_GUIDE.md)

## 📁 Project Structure

All training scripts are located in the `training/` directory:
- `training/run_master_pipeline.py` - Main pipeline orchestration script
- `training/spark_etl_master.py` - ETL processing
- `training/spark_analysis_master.py` - Data analytics
- `training/spark_ml_master_max.py` & `spark_ml_master_min.py` - Single model training
- `training/spark_ml_multi_models_max.py` & `spark_ml_multi_models_min.py` - Multi-model comparison

Run in the `training/` directory, or use `python training/xxx.py` from project root

---

## 🎓 Applicable Scenarios

- **Course Project**: EECS-6893 Big Data Analysis
- **Academic Paper**: Music industry pricing research
- **Business Application**: Ticketing platform pricing system
- **Technical Demo**: Spark + ML complete pipeline

---

**Authors**: EECS-6893 Project Team  
**Data Period**: 2017  
**Last Updated**: December 18, 2025
