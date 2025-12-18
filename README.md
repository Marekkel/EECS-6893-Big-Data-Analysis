# EECS-6893 Big Data Analysis - 项目文档

---

## 项目简介

本项目基于 **多源音乐活动数据集**（整合 Ticketmaster、SeatGeek、StubHub、Spotify 数据）构建了完整的大数据分析流程，涵盖 **ETL 处理、多维度统计分析和多模型机器学习票价预测**。通过 **Apache Spark 分布式计算框架** 和 **Google Cloud Dataproc**，实现了对大规模音乐活动数据的智能分析与**一级市场票价预测建模**（基于艺人热度、地点、时间等特征预浌 Ticketmaster 最高价和最低价）。

---

## 核心功能

1. **分布式 ETL 处理 (`spark_etl_master.py`)**
   - 解析 master_df.csv 中的列表字段（艺术家、Spotify 数据）
   - 类型转换与特征工程（价格范围、Spotify 数据标记、二级市场标记）
   - 输出按年月分区的 Parquet 格式数据

2. **多维度统计分析 (`spark_analysis_master.py`)**
   - **6 大分析维度**：年度类型趋势、城市热度 Top 50、艺术家人气 Top 100、星期分布、二级市场溢价、各州价格对比
   - 输出 CSV 分析报告

3. **单模型训练 (`spark_ml_master.py`)**
   - 支持 3 种算法选择：Random Forest / GBT / Linear Regression
   - 特征工程：StringIndexer + OneHotEncoder + StandardScaler
   - 预测目标：Ticketmaster 最高票价（tm_price_max）或最低票价（tm_price_min）
   - 支持选择预测目标：--target max 或 --target min

4. **多模型对比训练 (`spark_ml_multi_models.py`)**
   - **6 大回归模型**：Linear Regression、Lasso、Elastic Net、Decision Tree、Random Forest、GBT
   - 自动对比 RMSE/MAE/R² 性能指标
   - 支持同时预测最高价和最低价（分别运行）
   - 输出特征重要性、预测样本、模型对比报告

5. **一键运行流程 (`run_master_pipeline.py`)**
   - 支持本地模式和 Dataproc 模式
   - 4 步完整流程：ETL → 分析 → 单模型训练 → 多模型对比
   - 自动上传至 GCS（Dataproc 模式）

---

## 项目结构

```
EECS-6893-Big-Data-Analysis/
├── spark_etl_master.py           # Spark ETL 处理脚本
├── spark_analysis_master.py      # Spark 多维度分析脚本
├── spark_ml_master.py            # 单模型训练脚本（支持 RF/GBT/LR）
├── spark_ml_multi_models.py      # 多模型对比训练脚本（6 种算法）
├── run_master_pipeline.py        # 流程编排脚本（本地/Dataproc）
├── README.md                     # 项目文档
├── QUICKSTART.md                 # 快速入门指南
├── OUTPUT_GUIDE.md               # 输出文件详细说明
├── DATAPROC_SETUP.md             # Dataproc 部署指南
├── dataproc_config.json          # Dataproc 配置文件
├── data/
│   └── master_df.csv             # 主数据集（5102 条记录，2017 音乐活动）
├── tools/
│   └── view_parquet.py           # Parquet 文件查看工具
└── [输出目录 - 本地/GCS]
    ├── master_parquet/           # ETL 处理后的 Parquet 数据
    ├── analytics/                # 6 个分析结果 CSV 文件
    ├── ml_results/               # 单模型训练结果
    └── ml_multi_models/          # 多模型训练结果
        ├── models/               # 6 个训练好的模型
        ├── predictions_sample/   # 各模型预测样本
        ├── feature_importance/   # 树模型特征重要性
        └── metrics_comparison/   # 模型性能对比报告
```

---

## 功能模块详解

### 1. 数据源：master_df.csv

**数据集概况：**
- **记录数：** 5102 条音乐活动数据
- **时间跨度：** 2017 年音乐活动
- **数据来源：** 整合 Ticketmaster、SeatGeek、StubHub、Spotify 四大平台数据

**核心字段：**
- **活动信息：** event_id, event_name, event_date, genre, subgenre, city, state, country
- **一级市场价格：** tm_price_min, tm_price_max（Ticketmaster）
- **二级市场价格：** sg_avg_price, sg_lowest_price, sh_list_price（SeatGeek/StubHub）
- **Spotify 数据：** artists（列表）, spotify_followers（列表）, spotify_popularity（列表）
- **地理信息：** latitude, longitude

**数据质量：**
- 所有活动均包含一级市场票价数据（100% 覆盖）
- 二级市场数据覆盖率：约 60%（仅用于分析，不用于预测）
- Spotify 艺术家数据覆盖率：约 75%（作为重要预测特征）

---

### 2. Spark ETL 处理 (`spark_etl_master.py`)

**功能：** 解析 CSV 数据，进行类型转换与特征工程

**主要处理：**
1. **列表字段解析：** 从字符串列表提取第一个元素（artists, spotify_followers, spotify_popularity）
2. **类型转换：** 价格字段转 DoubleType，日期字段转 DateType，坐标转 DoubleType
3. **特征工程：**
   - `price_range`：Ticketmaster 价格区间（max - min）
   - `has_spotify_data`：是否有 Spotify 数据（布尔值）
   - `has_secondary_market`：是否有二级市场数据（布尔值）
4. **时间特征提取：** year, month, weekday

**运行方式：**
```bash
# 本地模式
python run_master_pipeline.py --mode local

# Dataproc 模式
python run_master_pipeline.py --mode dataproc
```

**输出：** `master_parquet/` - 按年月分区的 Parquet 数据

---

### 3. Spark 数据分析 (`spark_analysis_master.py`)

**功能：** 6 大维度统计分析，生成业务洞察报告

**分析维度：**

1. **events_per_year_genre** - 年度类型趋势分析
   - 各音乐类型的年度活动数量统计

2. **top_cities** - 城市热度 Top 50 排名
   - 按活动数量排序的城市排名

3. **top_artists** - 艺术家人气 Top 100 排名
   - 基于 Spotify 粉丝数和人气值的综合排名

4. **events_per_weekday** - 星期分布分析
   - 各星期几的活动数量统计

5. **secondary_market_by_genre** - 二级市场溢价分析
   - 各音乐类型的平均一级/二级市场价格对比
   - 溢价率计算（secondary_premium）

6. **price_by_state** - 各州价格对比
   - 各州的平均票价统计

**输出：** `analytics/` - 6 个 CSV 文件

---

### 4. 单模型训练 (`spark_ml_master.py`)

**功能：** 训练单个机器学习模型预浌 Ticketmaster 一级市场票价

**预测目标：**
- `tm_price_max`：Ticketmaster 最高票价（通过 --target max 指定）
- `tm_price_min`：Ticketmaster 最低票价（通过 --target min 指定）

**特征工程：**
- **类别特征：** genre, subgenre, state
- **数值特征：** spotify_popularity, spotify_followers, year, month, weekday, latitude, longitude
- **编码方式：** StringIndexer + OneHotEncoder + StandardScaler
- **注意：** 不再使用一级市场价格和二级市场价格作为特征，仅使用艺人热度、地点、时间等基础特征

**支持算法：**
- `--model-type rf`：Random Forest（默认）
- `--model-type gbt`：Gradient Boosted Trees
- `--model-type lr`：Linear Regression

**运行示例：**
```bash
# 训练随机森林模型（预测最高价）
python run_master_pipeline.py --mode local --model-type rf --target max

# 训练随机森林模型（预测最低价）
python run_master_pipeline.py --mode local --model-type rf --target min
```

**输出：**
- `ml_results/predictions_max/` 或 `predictions_min/`：测试集预浌结果
- `ml_results/metrics_max/` 或 `metrics_min/`：评估指标（RMSE/MAE/R²）
- `ml_results/models/`：训练好的模型
- `ml_results/feature_importance_max/` 或 `feature_importance_min/`：特征重要性（树模型）

---

### 5. 多模型对比训练 (`spark_ml_multi_models.py`)

**功能：** 同时训练 6 种回归模型，自动对比性能

**6 大回归模型：**
1. **Linear Regression** - 线性回归（基准模型）
2. **Lasso (α=0.1)** - L1 正则化线性回归
3. **Elastic Net (α=0.1, λ=0.5)** - L1+L2 正则化
4. **Decision Tree (depth=10)** - 决策树回归
5. **Random Forest (100 trees, depth=10)** - 随机森林
6. **Gradient Boosted Trees (100 trees, depth=5)** - 梯度提升树

**评估指标：**
- RMSE（均方根误差）- 越小越好
- MAE（平均绝对误差）- 越小越好
- R²（决定系数）- 越大越好（最大值 1.0）

**输出结构：**
```
ml_multi_models/
├── models_max/                  # 预测最高价的6个模型
│   ├── LinearRegression/
│   ├── Lasso/
│   ├── ElasticNet/
│   ├── DecisionTree/
│   ├── RandomForest/
│   └── GBT/
├── models_min/                  # 预测最低价的6个模型
├── predictions_sample_max/      # 最高价预浌样本
├── predictions_sample_min/      # 最低价预测样本
├── feature_importance_max/      # 最高价特征重要性
├── feature_importance_min/      # 最低价特征重要性
├── metrics_comparison_max_csv/  # 最高价模型对比
└── metrics_comparison_min_csv/  # 最低价模型对比
```

**关键指标对比文件：** `metrics_comparison_max_csv/` 和 `metrics_comparison_min_csv/`
```
Target         Model               RMSE    MAE     R²
tm_price_max   LinearRegression    105.67  78.23   0.721
tm_price_max   RandomForest        92.45   65.89   0.798
tm_price_min   LinearRegression    45.32   32.15   0.765
tm_price_min   RandomForest        38.21   25.43   0.812
...
```

---

### 6. 流程编排 (`run_master_pipeline.py`)

**功能：** 一键运行完整分析流程

**支持模式：**
- `--mode local`：本地 Spark 模式
- `--mode dataproc`：Google Cloud Dataproc 模式

**4 步完整流程：**
1. **Step 1 - ETL：** 解析 master_df.csv → master_parquet/
2. **Step 2 - Analytics：** 统计分析 → analytics/（6 个 CSV）
3. **Step 3 - Single ML：** 单模型训练 → ml_results/
4. **Step 4 - Multi ML：** 多模型对比 → ml_multi_models/

**Dataproc 模式特性：**
- 自动上传脚本和数据到 GCS
- 自动提交 4 个 Dataproc 作业
- 实时显示作业状态

**运行示例：**
```bash
# 本地完整流程
python run_master_pipeline.py --mode local

# Dataproc 完整流程（需先配置 dataproc_config.json）
python run_master_pipeline.py --mode dataproc

# 仅运行单模型训练（随机森林）
python run_master_pipeline.py --mode local --model-type rf
```

---

## 技术栈

- **数据处理：** Apache Spark (PySpark 3.x)
- **机器学习：** Spark MLlib (6 种回归算法)
- **云平台：** Google Cloud Platform (Dataproc, Cloud Storage)
- **数据格式：** CSV, Parquet
- **编程语言：** Python 3.7+

---

## 环境要求

### 本地环境
```bash
# 安装 PySpark
pip install pyspark

# 可选：安装本地 Spark（用于大规模数据处理）
# 下载地址：https://spark.apache.org/downloads.html
```

### Google Cloud Dataproc
- Python 3.7+
- PySpark 3.x
- Spark MLlib
- 配置 `dataproc_config.json`

详细配置指南：**[DATAPROC_SETUP.md](DATAPROC_SETUP.md)**

---

## 快速开始

### 方法 1：本地运行（推荐用于开发测试）

```bash
# 完整流程（ETL → 分析 → 单模型 → 多模型）
python run_master_pipeline.py --mode local

# 仅运行 ETL
python spark_etl_master.py \
  --input data/master_df.csv \
  --output master_parquet

# 仅运行分析
python spark_analysis_master.py \
  --input master_parquet \
  --output analytics

# 仅运行单模型训练（随机森林）
python spark_ml_master.py \
  --input master_parquet \
  --output ml_results \
  --model-type rf

# 仅运行多模型对比
python spark_ml_multi_models.py \
  --input master_parquet \
  --output ml_multi_models
```

---

### 方法 2：Google Cloud Dataproc（推荐用于生产环境）

#### Step 1: 配置 Dataproc

```bash
# 1. 编辑配置文件
# 填入你的 GCP 项目信息、集群名称、GCS bucket 等
vim dataproc_config.json

# 2. 创建 Dataproc 集群（如果还没有）
gcloud dataproc clusters create your-cluster-name \
  --region=us-east1 \
  --num-workers=2 \
  --master-machine-type=n1-standard-4 \
  --worker-machine-type=n1-standard-4
```

详细配置指南：**[DATAPROC_SETUP.md](DATAPROC_SETUP.md)**

#### Step 2: 一键运行完整流程

```bash
# Dataproc 模式（自动上传数据、提交 4 个作业）
python run_master_pipeline.py --mode dataproc
```

**这会自动执行：**
1. ✅ 上传 master_df.csv 到 GCS
2. ✅ 上传 4 个 Spark 脚本到 GCS
3. ✅ 提交 ETL 作业
4. ✅ 提交分析作业
5. ✅ 提交单模型训练作业
6. ✅ 提交多模型对比作业

---

## 输出文件说明

详细的输出文件说明，请参考：**[OUTPUT_GUIDE.md](OUTPUT_GUIDE.md)**

### 核心输出目录：

1. **master_parquet/** - ETL 处理后的 Parquet 数据
   - 按 year/month 分区存储
   - 高效列式存储格式

2. **analytics/** - 6 个统计分析 CSV 文件
   - `events_per_year_genre.csv`：年度类型趋势
   - `top_cities.csv`：城市热度 Top 50
   - `top_artists.csv`：艺术家人气 Top 100
   - `events_per_weekday.csv`：星期分布
   - `secondary_market_by_genre.csv`：二级市场溢价分析
   - `price_by_state.csv`：各州价格对比

3. **ml_results/** - 单模型训练结果
   - `predictions/`：测试集预测结果
   - `metrics/`：评估指标（RMSE/MAE/R²）
   - `models/`：训练好的模型
   - `feature_importance/`：特征重要性

4. **ml_multi_models/** - 多模型对比结果
   - `models/`：6 个训练好的模型
   - `predictions_sample/`：各模型预测样本
   - `feature_importance/`：树模型特征重要性
   - `metrics_comparison/`：**模型性能对比报告**（核心文件）

---

## 项目亮点

✅ **多源数据整合**：融合 Ticketmaster、SeatGeek、StubHub、Spotify 四大平台数据  
✅ **完整大数据处理流程**：从 ETL 到分析建模全流程覆盖  
✅ **多模型对比训练**：6 种回归算法自动对比，找出最优模型  
✅ **分布式计算**：利用 Apache Spark 处理大规模数据  
✅ **云原生架构**：支持 Google Cloud Dataproc 部署  
✅ **多维度分析**：地域、时间、类型、艺术家人气、二级市场溢价  
✅ **一级市场票价预测**：基于艺人热度、地点、时间预浌 Ticketmaster 最高价和最低价  
✅ **一键运行流程**：支持本地和 Dataproc 两种模式  
✅ **多目标预测**：同时支持预测最高价和最低价，全面了解价格区间

---

## 使用场景

1. **音乐活动市场分析**：了解不同城市、音乐类型的市场热度
2. **艺术家人气排名**：基于 Spotify 数据分析艺术家人气
3. **一级市场票价预浌**：基于艺人热度、地点、时间预浌 Ticketmaster 票价，辅助主办方定价决策
4. **二级市场溶价分析**：分析不同音乐类型的溶价情况
5. **时间趋势分析**：了解活动在不同时间段的分布规律
6. **特征重要性分析**：了解哪些因素（艺人热度、地域、类型等）对票价影响最大

---

## 文档索引

- **[README.md](README.md)** - 项目主文档（当前文件）
- **[QUICKSTART.md](QUICKSTART.md)** - 快速入门指南
- **[OUTPUT_GUIDE.md](OUTPUT_GUIDE.md)** - 输出文件详细说明
- **[DATAPROC_SETUP.md](DATAPROC_SETUP.md)** - Dataproc 部署指南

---

## 常见问题

### Q1: 如何选择运行模式？
- **本地模式：** 适合开发测试、数据量较小的场景
- **Dataproc 模式：** 适合生产环境、数据量较大的场景

### Q2: 如何查看模型性能对比？
查看 `ml_multi_models/metrics_comparison/metrics_comparison.csv` 文件，对比 6 个模型的 RMSE/MAE/R² 指标。

### Q3: 如何查看特征重要性？
树模型（Decision Tree、Random Forest、GBT）的特征重要性保存在 `ml_multi_models/feature_importance/` 目录。

### Q4: 如何修改模型参数？
编辑 `spark_ml_master.py` 或 `spark_ml_multi_models.py`，修改模型超参数（如树的数量、深度等）。

### Q5: 如何处理更大的数据集？
将数据上传到 GCS，使用 Dataproc 模式运行，并根据数据规模调整集群配置。

---

## 作者

EECS-6893 Big Data Analysis - Final Project

---

## License

MIT License
