# EECS-6893-Big-Data-Analysis

**Ticketmaster 音乐活动大数据分析项目**

Final Project - 基于 Ticketmaster API 的音乐活动数据采集、ETL、分析和机器学习预测

---

## 项目简介

本项目通过 Ticketmaster Discovery API 采集美国各主要城市的音乐活动数据，利用 Apache Spark 在 Google Cloud Dataproc 集群上进行大数据处理、分析和机器学习建模，挖掘音乐活动的地域分布、时间趋势、艺术家热度等洞察。

## 项目结构

```
EECS-6893-Big-Data-Analysis/
├── fetch_data.py              # 数据采集脚本：从 Ticketmaster API 抓取音乐活动数据
├── parse_data.py              # 本地数据解析脚本：将 JSON 转换为 Pandas DataFrame
├── spark_etl_events.py        # Spark ETL 作业：清洗和转换原始数据为 Parquet
├── spark_analysis_events.py   # Spark 分析作业：生成多维度统计分析
├── spark_ml_events.py         # Spark ML 作业：预测艺术家未来热度
├── README.md                  # 项目说明文档
└── ticketmaster_raw/          # 原始数据存储目录
    └── dt=2025-11-21/         # 按日期分区
        ├── events_AllUS.jsonl
        ├── events_Chicago.jsonl
        ├── events_Houston.jsonl
        ├── events_LosAngeles.jsonl
        ├── events_NewYork.jsonl
        └── events_Phoenix.jsonl
```

---

## 功能模块

### 1. 数据采集 (`fetch_data.py`)

**功能：** 从 Ticketmaster Discovery API 获取音乐活动数据

**特性：**
- 支持多个 DMA（Designated Market Area）区域
  - New York (345)
  - Los Angeles (324)
  - Chicago (249)
  - Houston (300)
  - Phoenix (359)
  - AllUS (200) - 全美范围
- 自动分页获取完整数据集
- 按日期分区存储（`dt=YYYY-MM-DD`）
- 速率限制保护，避免 API 限流
- 输出格式：JSONL（每行一个 JSON 对象）

**使用方法：**
```bash
python fetch_data.py
```

**配置：**
- API Key：在代码中设置 `API_KEY`
- 输出目录：`OUTPUT_DIR = "ticketmaster_raw/"`

---

### 2. 本地数据解析 (`parse_data.py`)

**功能：** 将原始 JSONL 文件解析为结构化的 Pandas DataFrame

**提取字段：**
- **基本信息：** ID, Name, Date
- **艺术家信息：** Artist, Segment, Genre, SubGenre
- **价格信息：** Price_Ranges_Type, Currency, Max, Min
- **场馆信息：** Venue, City, State, Country, Timezone
- **热度指标：** Upcoming_Events_Venue, Upcoming_Events_Artist
- **推广信息：** Promoter

**使用方法：**
```python
python parse_data.py
```

---

### 3. Spark ETL (`spark_etl_events.py`)

**功能：** 分布式数据清洗和转换，生成标准化的 Parquet 数据集

**处理流程：**
1. 读取原始 JSONL 文件
2. 扁平化嵌套 JSON 结构
3. 提取关键字段（venue、artist、classification）
4. 类型转换（日期、数值）
5. 去重（按活动 ID）
6. 过滤缺失关键字段的记录
7. 输出为 Parquet 格式

**使用方法（Google Cloud Dataproc）：**
```bash
gcloud dataproc jobs submit pyspark spark_etl_events.py \
  --cluster=<your-cluster> \
  --region=us-east1 \
  -- --input gs://<bucket>/ticketmaster_raw/dt=*/events_*.jsonl \
     --output gs://<bucket>/ticketmaster_processed/events_parquet
```

**必需参数：**
- `--input`: 原始数据路径（支持通配符）
- `--output`: 清洗后 Parquet 输出路径

---

### 4. Spark 数据分析 (`spark_analysis_events.py`)

**功能：** 多维度统计分析，生成业务洞察

**分析维度：**
1. **年度 & 类型分析：** 各年份各音乐类型的活动数量
2. **地域分析：** Top 50 活动最多的城市
3. **艺术家热度：** Top 50 即将举办活动最多的艺术家
4. **时间趋势：** 各星期几的活动分布

**使用方法（Google Cloud Dataproc）：**
```bash
gcloud dataproc jobs submit pyspark spark_analysis_events.py \
  --cluster=<your-cluster> \
  --region=us-east1 \
  -- --input gs://<bucket>/ticketmaster_processed/events_parquet \
     --output gs://<bucket>/ticketmaster_analytics
```

**输出文件：**
- `events_per_year_genre/`: 年度类型统计
- `top_cities/`: Top 城市排名
- `top_artists/`: Top 艺术家排名
- `events_per_weekday/`: 星期分布

---

### 5. Spark 机器学习 (`spark_ml_events.py`)

**功能：** 基于随机森林回归模型预测艺术家未来热度

**目标变量：**
- `Upcoming_Events_Artist`：艺术家即将举办的活动数量（热度代理指标）

**特征工程：**
- **类别特征：** Segment, Genre, SubGenre, Venue_City, Venue_State, Venue_Country
- **数值特征：** Upcoming_Events_Venue, year, month, weekday
- **编码方式：** StringIndexer + OneHotEncoder

**模型配置：**
- 算法：RandomForestRegressor
- 树数量：80
- 最大深度：10
- 训练/测试划分：80% / 20%

**评估指标：**
- RMSE（均方根误差）
- MAE（平均绝对误差）
- R²（决定系数）

**使用方法（Google Cloud Dataproc）：**
```bash
gcloud dataproc jobs submit pyspark spark_ml_events.py \
  --cluster=<your-cluster> \
  --region=us-east1 \
  -- --input gs://<bucket>/ticketmaster_processed/events_parquet \
     --metrics-output gs://<bucket>/ticketmaster_ml/metrics \
     --model-output gs://<bucket>/ticketmaster_ml/models/rf_upcoming_artist
```

**输出：**
- 训练好的模型（PipelineModel）
- 评估指标 JSON 文件

---

## 技术栈

- **数据采集：** Python, Requests, Ticketmaster Discovery API
- **数据处理：** Apache Spark (PySpark)
- **机器学习：** Spark MLlib (RandomForest, Pipeline)
- **云平台：** Google Cloud Platform (Dataproc, Cloud Storage)
- **数据格式：** JSONL, Parquet, CSV
- **本地分析：** Pandas, NumPy

---

## 环境要求

### 本地环境
```bash
pip install requests pandas numpy
```

### Spark 环境（Dataproc）
- Python 3.7+
- PySpark 3.x
- Spark MLlib

---

## 数据流程

```
1. 数据采集
   Ticketmaster API → fetch_data.py → ticketmaster_raw/*.jsonl

2. ETL 处理
   JSONL → spark_etl_events.py → Parquet (清洗后数据)

3. 分析 & ML
   ├── spark_analysis_events.py → 统计分析结果 (CSV)
   └── spark_ml_events.py → 预测模型 + 评估指标
```

---

## 快速开始

### 步骤 1：采集数据
```bash
# 配置 API Key 后运行
python fetch_data.py
```

### 步骤 2：提交 Spark ETL 作业
```bash
# 上传原始数据到 GCS
gsutil -m cp -r ticketmaster_raw/ gs://<your-bucket>/

# 提交 ETL 作业
gcloud dataproc jobs submit pyspark spark_etl_events.py \
  --cluster=<cluster-name> \
  --region=us-east1 \
  -- --input gs://<bucket>/ticketmaster_raw/dt=*/events_*.jsonl \
     --output gs://<bucket>/ticketmaster_processed/events_parquet
```

### 步骤 3：运行分析作业
```bash
gcloud dataproc jobs submit pyspark spark_analysis_events.py \
  --cluster=<cluster-name> \
  --region=us-east1 \
  -- --input gs://<bucket>/ticketmaster_processed/events_parquet \
     --output gs://<bucket>/ticketmaster_analytics
```

### 步骤 4：训练 ML 模型
```bash
gcloud dataproc jobs submit pyspark spark_ml_events.py \
  --cluster=<cluster-name> \
  --region=us-east1 \
  -- --input gs://<bucket>/ticketmaster_processed/events_parquet \
     --metrics-output gs://<bucket>/ticketmaster_ml/metrics \
     --model-output gs://<bucket>/ticketmaster_ml/models/rf_upcoming_artist
```

---

## 数据字段说明

| 字段名 | 类型 | 说明 |
|--------|------|------|
| ID | string | 活动唯一标识符 |
| Name | string | 活动名称 |
| Date | string | 活动日期 (YYYY-MM-DD) |
| Artist | string | 艺术家名称 |
| Segment | string | 活动大类（如 Music） |
| Genre | string | 音乐类型（如 Rock, Pop） |
| SubGenre | string | 音乐子类型 |
| Promoter | string | 推广商名称 |
| Venue | string | 场馆名称 |
| Venue_City | string | 场馆所在城市 |
| Venue_State | string | 场馆所在州（州代码） |
| Venue_Country | string | 场馆所在国家 |
| Venue_Timezone | string | 场馆时区 |
| Upcoming_Events_Venue | int | 该场馆即将举办的活动数 |
| Upcoming_Events_Artist | int | 该艺术家即将举办的活动数 |

---

## 项目亮点

✅ **完整的大数据处理流程**：从数据采集到分析建模  
✅ **分布式计算**：利用 Spark 处理大规模数据  
✅ **云原生架构**：基于 GCP Dataproc 和 Cloud Storage  
✅ **多维度分析**：地域、时间、类型、艺术家热度  
✅ **机器学习应用**：预测艺术家未来热度趋势  
✅ **可扩展设计**：支持新增 DMA 区域和特征维度

---

## 作者

EECS-6893 Big Data Analysis - Final Project

---

## License

MIT License
