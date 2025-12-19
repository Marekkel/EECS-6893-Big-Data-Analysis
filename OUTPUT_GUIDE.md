# Project Output Files Guide

After running `python training/run_master_pipeline.py --mode [local/dataproc]`, the following file structure will be generated:

---

## 📂 Output Directory Structure

### Local Mode (--mode local)
```
output/
├── master_parquet/              # Step 1: ETL cleaned data
│   ├── year=2017/
│   │   ├── month=1/
│   │   │   └── part-xxxxx.snappy.parquet
│   │   ├── month=2/
│   │   ├── month=3/
│   │   └── ...
│   └── year=2018/
│       └── ...
│
├── analytics/                   # Step 2: Statistical analysis results
│   ├── events_per_year_genre/
│   │   └── part-00000-xxxxx.csv
│   ├── top_cities/
│   │   └── part-00000-xxxxx.csv
│   ├── top_artists/
│   │   └── part-00000-xxxxx.csv
│   ├── events_per_weekday/
│   │   └── part-00000-xxxxx.csv
│   ├── secondary_market_by_genre/
│   │   └── part-00000-xxxxx.csv
│   └── price_by_state/
│       └── part-00000-xxxxx.csv
│
├── ml_results_max/              # Step 3: Single model ML results (max price)
│   ├── predictions/
│   │   └── part-00000-xxxxx.csv (prediction results)
│   ├── metrics/
│   │   └── part-00000-xxxxx.csv (evaluation metrics)
│   ├── feature_importance/
│   │   └── part-00000-xxxxx.csv (feature importance)
│   └── avg_encoding_max/        # Average encoding mappings
│
├── ml_results_min/              # Step 5: Single model ML results (min price)
│   ├── predictions/
│   ├── metrics/
│   ├── feature_importance/
│   └── avg_encoding_min/
│
├── ml_multi_models_max/         # Step 4: Multi-model comparison results (max price)
│   ├── models/                  # 6 model files
│   │   ├── linear_regression/
│   │   ├── lasso_regression/
│   │   ├── elastic_net/
│   │   ├── decision_tree/
│   │   ├── random_forest/
│   │   └── gbt/
│   ├── predictions_sample/      # Prediction samples for each model
│   ├── feature_importance/      # Feature importance for tree models
│   ├── metrics_comparison_csv/  # All model comparison (CSV)
│   └── metrics_comparison_json/ # All model comparison (JSON)
│
└── ml_multi_models_min/         # Step 6: Multi-model comparison results (min price)
    ├── models/
    ├── predictions_sample/
    ├── feature_importance/
    ├── metrics_comparison_csv/
    └── metrics_comparison_json/
```

### Dataproc Mode (--mode dataproc)
```
gs://your-bucket/
├── data/
│   └── master_df.csv            # Uploaded raw data
├── scripts/
│   ├── spark_etl_master.py      # Uploaded scripts
│   ├── spark_analysis_master.py
│   └── spark_ml_master_*.py
└── output/
    ├── master_parquet/          # Same structure as local mode
    ├── analytics/
    └── ml_results_*/
```

---

## 📊 Detailed File Descriptions

### 1️⃣ ETL Output (`master_parquet/`)

**File Format**: Parquet (columnar storage, efficient compression)
**Partitioning**: By year and month

**Included Fields**:
- **Basic Info**: event_id, event_title, artist, event_date
- **Venue Info**: venue, city, state, venue_lat, venue_long
- **Classification**: genre, subgenre, event_type
- **Price Data**:
  - Ticketmaster: tm_min_price, tm_max_price, price_range
  - SeatGeek: sg_avg_price, sg_min_price, sg_max_price, sg_listing_count
  - StubHub: sh_max_price, sh_min_price, sh_total_postings, sh_total_tickets
- **Spotify Data**: spotify_followers, spotify_popularity, has_spotify_data
- **Time Features**: year, month, weekday, is_weekend
- **Market Features**: has_secondary_market

**Usage**: 
- Standardized data source for subsequent analysis and ML
- Support for fast Spark SQL queries
- Directly usable in visualization tools (e.g., Tableau, PowerBI)

---

### 2️⃣ Analytics Output (`analytics/`)

#### 📈 `events_per_year_genre/part-00000.csv`
**Columns**: year, genre, event_count, avg_price, avg_popularity

**Sample Data**:
```csv
year,genre,event_count,avg_price,avg_popularity
2017,Rock,850,45.30,68.5
2017,Pop,620,52.10,72.3
2018,Country,540,38.50,65.2
```

**Usage**: 
- Generate time trend charts (which music genres are growing)
- Analyze price changes for different genres over years
- Correlation between Spotify popularity and event volume

---

#### 🏙️ `top_cities/part-00000.csv`
**Columns**: city, state, event_count, unique_artists, avg_price, secondary_market_events

**Sample Data**:
```csv
city,state,event_count,unique_artists,avg_price,secondary_market_events
New York,NY,450,280,68.50,320
Los Angeles,CA,380,245,62.30,290
Nashville,TN,320,190,42.10,250
```

**Usage**:
- Map visualization (US music event heatmap)
- Identify music industry hub cities
- Compare prices and secondary market activity between cities

---

#### 🎤 `top_artists/part-00000.csv`
**Columns**: artist, event_count, avg_spotify_popularity, spotify_followers, avg_ticket_price, cities_performed

**Sample Data**:
```csv
artist,event_count,avg_spotify_popularity,spotify_followers,avg_ticket_price,cities_performed
Taylor Swift,45,95.5,85000000,125.50,28
Ed Sheeran,38,92.3,78000000,98.30,25
Bruno Mars,32,88.7,65000000,102.00,22
```

**Usage**:
- Artist ranking and popularity analysis
- Study relationship between Spotify followers and ticket prices
- Tour scale analysis (cities_performed)

---

#### 📅 `events_per_weekday/part-00000.csv`
**Columns**: weekday, weekday_name, is_weekend, event_count, avg_price

**Sample Data**:
```csv
weekday,weekday_name,is_weekend,event_count,avg_price
1,Sunday,true,420,52.30
2,Monday,false,180,38.50
6,Friday,false,650,58.20
7,Saturday,true,820,65.80
```

**Usage**:
- Weekend vs. weekday event distribution bar chart
- Analyze relationship between price and date
- Event planning decisions (best performance day selection)

---

#### 💰 `secondary_market_by_genre/part-00000.csv`
**Columns**: genre, event_count, avg_seatgeek_price, avg_stubhub_max, avg_tm_price, avg_premium_pct

**Sample Data**:
```csv
genre,event_count,avg_seatgeek_price,avg_stubhub_max,avg_tm_price,avg_premium_pct
Rock,650,85.50,120.30,52.30,63.5
Pop,520,98.20,145.60,58.70,67.3
Country,480,62.30,88.40,42.10,48.0
```

**Usage**:
- Analyze secondary market premium situation
- Ticket scalping profit margins for different music genres
- Ticketmaster vs. secondary market price comparison

---

#### 🗺️ `price_by_state/part-00000.csv`
**Columns**: state, event_count, min_price, avg_price, max_price, avg_price_range

**Sample Data**:
```csv
state,event_count,min_price,avg_price,max_price,avg_price_range
CA,850,15.00,68.50,350.00,42.30
NY,720,20.00,72.30,420.00,48.50
TX,580,12.00,52.10,280.00,35.20
```

**Usage**:
- US state-wise ticket price map
- Geographic economic difference analysis
- High-price/low-price market identification

---

### 3️⃣ Single Model Machine Learning Output (`ml_results_max/` & `ml_results_min/`)

#### 🔮 `predictions/part-00000.csv`
**Columns**: event_id, artist, genre, city, event_date, tm_min_price, prediction, spotify_popularity

**Sample Data**:
```csv
event_id,artist,genre,city,event_date,actual_price,prediction,spotify_popularity
Z7r9jZ1AdF8KP,Imagine Dragons,Rock,Boston,2017-08-15,89.0,118.30,85
vvG1iZ9Q89yI8,Ariana Grande,Pop,Miami,2017-09-22,95.0,138.90,92
```

**Usage**:
- Evaluate model prediction accuracy (actual vs predicted)
- Identify anomalous events with large prediction errors
- Provide references for pricing new events

---

#### 📊 `metrics/part-00000.csv`
**Columns**: metric, value

**Sample Data**:
```csv
metric,value
RMSE,15.32
MAE,11.85
R2,0.8245
train_size,3200
test_size,800
```

**Metric Explanations**:
- **RMSE** (Root Mean Squared Error): Prediction error root mean square, smaller is better (unit: dollars)
- **MAE** (Mean Absolute Error): Average absolute error, average deviation amount
- **R²** (R-squared): Model fit, between 0-1, closer to 1 is better
- **train_size/test_size**: Training set and test set sizes

**Usage**:
- Model performance evaluation
- Compare different models (rf vs gbt vs lr)
- Key metrics for project reports

---

#### ⭐ `feature_importance/part-00000.csv`
**Columns**: feature, importance

**Sample Data**:
```csv
feature,importance
spotify_popularity,0.2850
city_avg_price,0.2340
state_avg_price,0.1820
genre_avg_price,0.1250
year,0.0980
month,0.0760
```

**Usage**:
- Identify the most critical factors affecting ticket prices
- Visualize as horizontal bar chart
- Explain model decision logic
- Business insights (which factors are most important)

---

### 4️⃣ Multi-Model Comparison Output (`ml_multi_models_max/` & `ml_multi_models_min/`)

#### 📊 `metrics_comparison_csv/part-00000.csv` ⭐ KEY FILE
**Columns**: model, rmse, mae, r2

**Sample Data**:
```csv
model,rmse,mae,r2
random_forest,93.7,42.1,0.617    ← Usually best for max price
gbt,95.2,43.5,0.605
linear_regression,105.6,48.2,0.521
```

**Usage**:
- **Primary use**: Compare 6 models to find the best one
- Create bar charts for visualization
- Key findings for project presentation

---

## 🔍 How to Find Files

### Local Mode
```powershell
# View all output
ls -R output/

# View CSV file content
Get-Content output/analytics/top_cities/part-00000-*.csv | Select-Object -First 20

# Open with Excel (find part-00000 prefix CSV files)
```

### Dataproc Mode
```powershell
# List GCS files
gsutil ls -r gs://your-bucket/output/

# Download all results to local
gsutil -m cp -r gs://your-bucket/output/ ./

# Download single file
gsutil cp gs://your-bucket/output/analytics/top_cities/*.csv ./
```

### View in GCP Console
1. Open https://console.cloud.google.com/storage/
2. Navigate to your bucket
3. Go to `output/` folder
4. Click files to preview or download directly

---

## 📈 Recommended Visualization Approaches

### Using these CSV files, you can create:

1. **Time Trend Charts** (`events_per_year_genre`)
   - Line chart: event volume changes by genre over time

2. **Geographic Heatmap** (`top_cities`, `price_by_state`)
   - US map: city event density
   - State-level price distribution

3. **Artist Leaderboard** (`top_artists`)
   - Horizontal bar chart: Top 20 artists
   - Scatter plot: Spotify followers vs. average ticket price

4. **Price Analysis** (`secondary_market_by_genre`, `price_by_state`)
   - Box plot: price distribution by genre
   - Bar chart: secondary market premium rate comparison

5. **ML Results Display** (`predictions`, `feature_importance`)
   - Scatter plot: actual price vs. predicted price
   - Horizontal bar chart: feature importance ranking

---

## 💡 Quick Validation of Output

After running, check these key files:

```powershell
# Check ETL output
ls output/master_parquet/year=2017/

# View analysis result row count (should have data)
(Get-Content output/analytics/top_cities/*.csv).Count

# View ML evaluation metrics
Get-Content output/ml_results_max/metrics/*.csv

# View multi-model comparison results
Get-Content output/ml_multi_models_max/metrics_comparison_csv/*.csv
```

---

## 🎓 Project Summary

### Complete Pipeline
1. **ETL**: Clean 5,102 event records, extract 30+ features
2. **Analytics**: 6-dimensional statistical analysis (year trends, city rankings, artist popularity, etc.)
3. **Single ML (MAX)**: RandomForest single model training (predict max price)
4. **Multi ML (MAX)**: 6 algorithm comparison (predict max price)
5. **Single ML (MIN)**: RandomForest single model training (predict min price)
6. **Multi ML (MIN)**: 6 algorithm comparison (predict min price)

### Output File Count
- **ETL**: 1 Parquet dataset (partitioned by year/month)
- **Analytics**: 6 CSV files (statistical analysis results)
- **Single ML**: 2 sets of results (max price + min price, 3-4 files per set)
- **Multi ML**: 2 sets of results (max price + min price, 20+ files per set)

### Technology Stack
- **Big Data Processing**: Apache Spark, PySpark
- **Machine Learning**: Spark MLlib (6 regression algorithms)
- **Cloud Platform**: Google Cloud Dataproc, GCS
- **Data Sources**: Ticketmaster, SeatGeek, StubHub, Spotify

All files are in **CSV format** (except Parquet and model files), directly openable with Excel, Python pandas, or visualization tools!

---

**Last Updated**: December 18, 2025
