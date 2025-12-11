# 🚀 快速使用指南 - 整合后的项目

## 📋 项目概述

基于 `master_df.csv`（5102 条 2017 年音乐活动数据）的完整大数据分析和机器学习项目。

**数据来源**: Ticketmaster + SeatGeek + StubHub + Spotify  
**目标任务**: 预测二级市场票价（SeatGeek 平均价格）

---

## 🎯 核心功能

### 1️⃣ ETL 数据清洗
- **脚本**: `spark_etl_master.py`
- **功能**: 解析 CSV，类型转换，特征工程
- **输出**: Parquet 格式（按年月分区）

### 2️⃣ 统计分析（6 个维度）
- **脚本**: `spark_analysis_master.py`
- **分析内容**:
  1. 年份 × 音乐类型趋势
  2. 热门城市 Top 50
  3. 热门艺术家 Top 100（Spotify 排序）
  4. 星期几活动分布
  5. 二级市场溢价分析
  6. 各州价格排名

### 3️⃣ 单模型 ML（RandomForest）
- **脚本**: `spark_ml_master.py`
- **模型**: Random Forest（可选 GBT, LR）
- **输出**: 预测结果 + 评估指标 + 特征重要性

### 4️⃣ 多模型对比训练 ⭐NEW
- **脚本**: `spark_ml_multi_models.py`
- **模型**: 6 种算法全面对比
  - Linear Regression（线性回归）
  - Lasso Regression（L1 正则化）
  - Elastic Net（L1+L2 正则化）
  - Decision Tree（决策树）
  - Random Forest（随机森林）
  - GBT（梯度提升树）
- **输出**: 
  - 6 个训练好的模型
  - 每个模型的预测样例
  - 树模型的特征重要性
  - 模型性能对比表（RMSE, MAE, R²）

---

## 🏃 运行方式

### 本地运行（需要 Spark 环境）
```powershell
python run_master_pipeline.py --mode local
```

**输出位置**: `output/` 文件夹

### Dataproc 运行（云端）
```powershell
python run_master_pipeline.py --mode dataproc
```

**输出位置**: `gs://your-bucket/output/`

---

## 📊 输出文件结构

```
output/
├── master_parquet/           # 清洗后的数据（Parquet）
├── analytics/                # 6 个统计分析 CSV
│   ├── events_per_year_genre/
│   ├── top_cities/
│   ├── top_artists/
│   ├── events_per_weekday/
│   ├── secondary_market_by_genre/
│   └── price_by_state/
├── ml_results/               # 单模型 ML 结果
│   ├── predictions/
│   ├── metrics/
│   └── feature_importance/
└── ml_multi_models/          # ⭐ 多模型对比结果
    ├── models/               # 6 种模型文件
    │   ├── linear_regression/
    │   ├── lasso_regression/
    │   ├── elastic_net/
    │   ├── decision_tree/
    │   ├── random_forest/
    │   └── gbt/
    ├── predictions_sample/   # 每个模型的预测样例
    ├── feature_importance/   # 树模型特征重要性
    └── metrics_comparison_csv/ # 模型性能对比表 ⭐
```

---

## 🔑 关键文件说明

### 📈 统计分析结果
| 文件 | 内容 | 用途 |
|------|------|------|
| `top_cities.csv` | 城市活动排名 | 地图热力图 |
| `top_artists.csv` | 艺术家排行榜 | Spotify 粉丝数 vs 票价分析 |
| `events_per_year_genre.csv` | 时间趋势 | 折线图：各类型音乐趋势 |
| `secondary_market_by_genre.csv` | 溢价率 | 柱状图：二级市场溢价对比 |

### 🤖 机器学习结果
| 文件 | 内容 | 用途 |
|------|------|------|
| **`metrics_comparison_csv/*.csv`** ⭐ | **6 种模型性能对比** | **找出最佳模型** |
| `predictions_sample/*/*.csv` | 每个模型的预测样例 | 散点图：actual vs predicted |
| `feature_importance/*/*.csv` | 特征重要性排名 | 横向柱状图：影响因素 |

---

## 📊 模型性能对比（示例）

运行完成后，查看对比结果：
```powershell
Get-Content output/ml_multi_models/metrics_comparison_csv/part-00000-*.csv
```

**预期输出**:
```csv
model,rmse,mae,r2
random_forest,15.32,11.85,0.8245    ← 通常最佳
gbt,16.18,12.30,0.8102
elastic_net,18.45,14.20,0.7856
lasso_regression,18.78,14.55,0.7801
linear_regression,19.20,15.10,0.7698
decision_tree,21.50,16.80,0.7320
```

**解读**:
- **RMSE 最小** = Random Forest（预测误差 $15.32）
- **R² 最高** = Random Forest（拟合度 82.45%）
- **线性模型** vs **树模型**: 树模型精度更高

---

## 🎨 可视化建议

### 1. 模型对比柱状图
- X 轴：6 种模型
- Y 轴：RMSE / MAE / R²
- 一眼看出最佳模型

### 2. 预测准确性散点图
- X 轴：actual_price
- Y 轴：predicted_price
- 对角线 = 完美预测
- 为每个模型绘制一张图，对比偏离程度

### 3. 特征重要性横向柱状图
- 对比 3 个树模型（DT, RF, GBT）
- 找出一致认为重要的特征
- 业务洞察：定价关键因素

### 4. 地理热力图
- 使用 `top_cities.csv`
- 美国地图标注活动密度

### 5. 时间趋势折线图
- 使用 `events_per_year_genre.csv`
- 各音乐类型活动数量随时间变化

---

## 🆚 单模型 vs 多模型

| 特性 | 单模型 (spark_ml_master.py) | 多模型 (spark_ml_multi_models.py) |
|------|---------------------------|--------------------------------|
| **模型数量** | 1 个（可选择算法） | 6 个（全面对比） |
| **训练时间** | 快 (~5 分钟) | 较慢 (~20 分钟) |
| **输出** | 单一结果 | 性能对比表 ⭐ |
| **用途** | 快速验证 | 最终报告/论文 |
| **特征重要性** | 有（如果是 RF/GBT） | 3 个树模型对比 |
| **推荐场景** | 开发阶段 | 项目交付阶段 |

---

## 💡 项目亮点

### 组员贡献整合 ✅
- 原始代码：针对旧 Ticketmaster API 结构
- 整合后：完美适配 `master_df.csv` 列名
- 新增功能：6 种模型自动训练 + 性能对比

### 技术优势
1. **大数据处理**: Spark 分布式计算
2. **多源数据融合**: TM + SG + SH + Spotify
3. **完整 ML Pipeline**: 数据清洗 → 特征工程 → 模型训练 → 评估
4. **云端可扩展**: 支持 Dataproc 部署

### 业务价值
- **定价策略**: 预测二级市场价格，指导原始定价
- **市场洞察**: 识别高价值城市和艺术家
- **溢价分析**: 二级市场利润空间

---

## 🐛 常见问题

### Q1: 本地运行需要什么环境？
**A**: Apache Spark 3.x + Python 3.7+

### Q2: Dataproc 运行需要修改什么？
**A**: 编辑 `dataproc_config.json`，填入你的 GCP 项目信息

### Q3: 多模型训练太慢怎么办？
**A**: 
- 本地模式：只运行单模型（更快）
- Dataproc：增加集群节点数

### Q4: 如何查看最佳模型？
**A**: 
```powershell
Get-Content output/ml_multi_models/metrics_comparison_csv/*.csv | Sort-Object
```
RMSE 最小的就是最佳模型

### Q5: 模型文件能做什么？
**A**: 可以加载用于新活动的价格预测（需要相同特征）

---

## 📚 详细文档

- **输出文件详解**: `OUTPUT_GUIDE.md`
- **Dataproc 部署**: `DATAPROC_SETUP.md`
- **项目说明**: `README.md`

---

## 🎓 适用场景

- **课程项目**: EECS-6893 Big Data Analysis
- **学术论文**: 音乐产业定价研究
- **业务应用**: 票务平台定价系统
- **技术展示**: Spark + ML 完整流程

---

**作者**: EECS-6893 项目组  
**数据时间**: 2017 年  
**最后更新**: 2025-12-11
