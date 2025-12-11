# 项目输出文件指南

运行 `python run_master_pipeline.py --mode [local/dataproc]` 后会生成以下文件结构：

---

## 📂 输出目录结构

### 本地模式 (--mode local)
```
output/
├── master_parquet/              # 步骤 1: ETL 清洗后的数据
│   ├── year=2017/
│   │   ├── month=1/
│   │   │   └── part-xxxxx.snappy.parquet
│   │   ├── month=2/
│   │   ├── month=3/
│   │   └── ...
│   └── year=2018/
│       └── ...
│
├── analytics/                   # 步骤 2: 统计分析结果
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
├── ml_results/                  # 步骤 3: 单模型机器学习结果
│   ├── predictions/
│   │   └── part-00000-xxxxx.csv (预测结果)
│   ├── metrics/
│   │   └── part-00000-xxxxx.csv (评估指标)
│   └── feature_importance/
│       └── part-00000-xxxxx.csv (特征重要性)
│
└── ml_multi_models/             # 步骤 4: 多模型对比训练结果
    ├── models/                  # 6 种模型文件
    │   ├── linear_regression/
    │   ├── lasso_regression/
    │   ├── elastic_net/
    │   ├── decision_tree/
    │   ├── random_forest/
    │   └── gbt/
    ├── predictions_sample/      # 每个模型的预测样例
    │   ├── linear_regression/
    │   ├── lasso_regression/
    │   ├── elastic_net/
    │   ├── decision_tree/
    │   ├── random_forest/
    │   └── gbt/
    ├── feature_importance/      # 树模型的特征重要性
    │   ├── decision_tree/
    │   ├── random_forest/
    │   └── gbt/
    ├── metrics_comparison_csv/  # 所有模型对比 (CSV)
    │   └── part-00000-xxxxx.csv
    └── metrics_comparison_json/ # 所有模型对比 (JSON)
        └── part-00000-xxxxx.json
```

### Dataproc 模式 (--mode dataproc)
```
gs://your-bucket/
├── data/
│   └── master_df.csv            # 上传的原始数据
├── scripts/
│   ├── spark_etl_master.py      # 上传的脚本
│   ├── spark_analysis_master.py
│   └── spark_ml_master.py
└── output/
    ├── master_parquet/          # 同本地模式结构
    ├── analytics/
    └── ml_results/
```

---

## 📊 详细文件说明

### 1️⃣ ETL 输出 (`master_parquet/`)

**文件格式**: Parquet (列式存储，高效压缩)
**分区方式**: 按年份/月份分区

**包含字段**:
- **基本信息**: event_id, event_title, artist, event_date
- **场馆信息**: venue, city, state, venue_lat, venue_long
- **分类**: genre, subgenre, event_type
- **价格数据**:
  - Ticketmaster: tm_min_price, tm_max_price, price_range
  - SeatGeek: sg_avg_price, sg_min_price, sg_max_price, sg_listing_count
  - StubHub: sh_max_price, sh_min_price, sh_total_postings, sh_total_tickets
- **Spotify 数据**: spotify_followers, spotify_popularity, has_spotify_data
- **时间特征**: year, month, weekday, is_weekend
- **市场特征**: has_secondary_market

**用途**: 
- 作为后续分析和 ML 的标准化数据源
- 支持 Spark SQL 快速查询
- 可直接用于可视化工具（如 Tableau, PowerBI）

---

### 2️⃣ 分析输出 (`analytics/`)

#### 📈 `events_per_year_genre/part-00000.csv`
**列**: year, genre, event_count, avg_price, avg_popularity

**示例数据**:
```csv
year,genre,event_count,avg_price,avg_popularity
2017,Rock,850,45.30,68.5
2017,Pop,620,52.10,72.3
2018,Country,540,38.50,65.2
```

**用途**: 
- 生成时间趋势图（哪些类型音乐越来越火）
- 分析不同年份音乐类型的价格变化
- Spotify 热度与活动数量的相关性

---

#### 🏙️ `top_cities/part-00000.csv`
**列**: city, state, event_count, unique_artists, avg_price, secondary_market_events

**示例数据**:
```csv
city,state,event_count,unique_artists,avg_price,secondary_market_events
New York,NY,450,280,68.50,320
Los Angeles,CA,380,245,62.30,290
Nashville,TN,320,190,42.10,250
```

**用途**:
- 地图可视化（美国音乐活动热力图）
- 识别音乐产业中心城市
- 比较城市间的价格和二级市场活跃度

---

#### 🎤 `top_artists/part-00000.csv`
**列**: artist, event_count, avg_spotify_popularity, spotify_followers, avg_ticket_price, cities_performed

**示例数据**:
```csv
artist,event_count,avg_spotify_popularity,spotify_followers,avg_ticket_price,cities_performed
Taylor Swift,45,95.5,85000000,125.50,28
Ed Sheeran,38,92.3,78000000,98.30,25
Bruno Mars,32,88.7,65000000,102.00,22
```

**用途**:
- 艺术家排名和热度分析
- Spotify 粉丝数与票价的关系研究
- 巡演规模分析（cities_performed）

---

#### 📅 `events_per_weekday/part-00000.csv`
**列**: weekday, weekday_name, is_weekend, event_count, avg_price

**示例数据**:
```csv
weekday,weekday_name,is_weekend,event_count,avg_price
1,Sunday,true,420,52.30
2,Monday,false,180,38.50
6,Friday,false,650,58.20
7,Saturday,true,820,65.80
```

**用途**:
- 周末 vs 工作日活动分布柱状图
- 分析价格与日期的关系
- 活动策划决策（最佳演出日选择）

---

#### 💰 `secondary_market_by_genre/part-00000.csv`
**列**: genre, event_count, avg_seatgeek_price, avg_stubhub_max, avg_tm_price, avg_premium_pct

**示例数据**:
```csv
genre,event_count,avg_seatgeek_price,avg_stubhub_max,avg_tm_price,avg_premium_pct
Rock,650,85.50,120.30,52.30,63.5
Pop,520,98.20,145.60,58.70,67.3
Country,480,62.30,88.40,42.10,48.0
```

**用途**:
- 分析二级市场溢价情况
- 不同类型音乐的倒票利润空间
- Ticketmaster vs 二级市场价格对比

---

#### 🗺️ `price_by_state/part-00000.csv`
**列**: state, event_count, min_price, avg_price, max_price, avg_price_range

**示例数据**:
```csv
state,event_count,min_price,avg_price,max_price,avg_price_range
CA,850,15.00,68.50,350.00,42.30
NY,720,20.00,72.30,420.00,48.50
TX,580,12.00,52.10,280.00,35.20
```

**用途**:
- 美国各州票价地图
- 地理经济差异分析
- 高价/低价市场识别

---

### 3️⃣ 单模型机器学习输出 (`ml_results/`)

#### 🔮 `predictions/part-00000.csv`
**列**: event_id, artist, genre, city, event_date, tm_min_price, sg_avg_price, prediction, spotify_popularity, spotify_followers

**示例数据**:
```csv
event_id,artist,genre,city,event_date,tm_min_price,sg_avg_price,prediction,spotify_popularity,spotify_followers
Z7r9jZ1AdF8KP,Imagine Dragons,Rock,Boston,2017-08-15,89.0,125.50,118.30,85,12500000
vvG1iZ9Q89yI8,Ariana Grande,Pop,Miami,2017-09-22,95.0,142.80,138.90,92,45000000
```

**用途**:
- 评估模型预测准确性（actual vs predicted）
- 识别预测误差大的异常活动
- 为新活动定价提供参考

---

#### 📊 `metrics/part-00000.csv`
**列**: metric, value

**示例数据**:
```csv
metric,value
RMSE,15.32
MAE,11.85
R2,0.8245
train_size,3200
test_size,800
```

**指标解释**:
- **RMSE** (Root Mean Squared Error): 预测误差均方根，越小越好（单位：美元）
- **MAE** (Mean Absolute Error): 平均绝对误差，平均偏差多少钱
- **R²** (R-squared): 模型拟合度，0-1 之间，越接近 1 越好
- **train_size/test_size**: 训练集和测试集大小

**用途**:
- 模型性能评估
- 对比不同模型（rf vs gbt vs lr）
- 项目报告中的关键指标展示

---

#### ⭐ `feature_importance/part-00000.csv`
**列**: feature, importance

**示例数据**:
```csv
feature,importance
spotify_popularity,0.2850
tm_min_price,0.2340
spotify_followers,0.1820
genre_vec,0.1250
sg_listing_count,0.0980
state_vec,0.0760
```

**用途**:
- 识别影响票价的最关键因素
- 可视化为横向柱状图
- 解释模型决策逻辑
- 业务洞察（哪些因素最重要）

---

## 🔍 如何查找文件

### 本地模式
```powershell
# 查看所有输出
ls -R output/

# 查看 CSV 文件内容
Get-Content output/analytics/top_cities/part-00000-*.csv | Select-Object -First 20

# 用 Excel 打开（找到 part-00000 开头的 CSV 文件）
```

### Dataproc 模式
```powershell
# 列出 GCS 文件
gsutil ls -r gs://your-bucket/output/

# 下载所有结果到本地
gsutil -m cp -r gs://your-bucket/output/ ./

# 下载单个文件
gsutil cp gs://your-bucket/output/analytics/top_cities/*.csv ./
```

### 在 GCP Console 查看
1. 打开 https://console.cloud.google.com/storage/
2. 进入你的 bucket
3. 导航到 `output/` 文件夹
4. 点击文件可以直接预览或下载

---

## 📈 推荐可视化方案

### 使用这些 CSV 文件可以创建：

1. **时间趋势图** (`events_per_year_genre`)
   - 折线图：各音乐类型随时间的活动数量变化

2. **地理热力图** (`top_cities`, `price_by_state`)
   - 美国地图：城市活动密度
   - 州级票价分布

3. **艺术家排行榜** (`top_artists`)
   - 横向柱状图：Top 20 艺术家
   - 散点图：Spotify 粉丝数 vs 平均票价

4. **价格分析** (`secondary_market_by_genre`, `price_by_state`)
   - 箱线图：各类型音乐价格分布
   - 柱状图：二级市场溢价率对比

5. **ML 结果展示** (`predictions`, `feature_importance`)
   - 散点图：实际价格 vs 预测价格
   - 横向柱状图：特征重要性排名

---

## 💡 快速验证输出

运行完成后，检查这些关键文件：

```powershell
# 检查 ETL 输出
ls output/master_parquet/year=2017/

# 查看分析结果行数（应该有数据）
(Get-Content output/analytics/top_cities/*.csv).Count

# 查看 ML 评估指标
Get-Content output/ml_results/metrics/*.csv

# 查看多模型对比结果
Get-Content output/ml_multi_models/metrics_comparison_csv/*.csv
```

---

## 🎓 项目总结

### 完整流程
1. **ETL**: 清洗 5102 条活动数据，提取 30+ 特征
2. **Analytics**: 6 个维度统计分析（年份趋势、城市排名、艺术家热度等）
3. **Single ML**: RandomForest 单模型训练
4. **Multi ML**: 6 种算法对比（Linear Regression, Lasso, Elastic Net, Decision Tree, Random Forest, GBT）

### 输出文件总数
- **ETL**: 1 个 Parquet 数据集（按年月分区）
- **Analytics**: 6 个 CSV 文件（统计分析结果）
- **Single ML**: 3 个文件（预测、指标、特征重要性）
- **Multi ML**: 20+ 文件（6 个模型 + 对比指标 + 样例预测 + 特征重要性）

### 技术栈
- **大数据处理**: Apache Spark, PySpark
- **机器学习**: Spark MLlib (6 种回归算法)
- **云平台**: Google Cloud Dataproc, GCS
- **数据源**: Ticketmaster, SeatGeek, StubHub, Spotify

所有文件都是 **CSV 格式**（除了 Parquet 和模型文件），可以直接用 Excel、Python pandas 或可视化工具打开！
