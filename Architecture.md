# 🏗️ System Architecture & Execution Guide

## Table of Contents
1. [System Architecture](#system-architecture)
2. [Data Flow Diagram](#data-flow-diagram)
3. [Execution Steps](#execution-steps)
4. [Screenshots Guide](#screenshots-guide)
5. [Troubleshooting](#troubleshooting)

---

## 🏛️ System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                   INTELLIGENT URBAN MOBILITY ANALYTICS                  │
│                         & GenAI INSIGHTS PLATFORM                       │
└─────────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────────┐
│                          LAYER 1: DATA INGESTION                      │
├───────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌──────────────┐         ┌──────────────────────┐                  │
│  │   Kaggle     │────────▶│  MobilityDataAnalyzer│                  │
│  │   Dataset    │         │       (OOP)          │                  │
│  │  (5GB CSV)   │         └──────────┬───────────┘                  │
│  └──────────────┘                    │                              │
│                                      ▼                               │
│                          ┌─────────────────────┐                     │
│                          │  Data Cleaning      │                     │
│                          │  - Remove nulls     │                     │
│                          │  - Fix timestamps   │                     │
│                          │  - Handle outliers  │                     │
│                          └──────────┬──────────┘                     │
│                                     │                                │
│                                     ▼                                │
│                          ┌─────────────────────┐                     │
│                          │ Feature Engineering │                     │
│                          │  - Time features    │                     │
│                          │  - KPI derivation   │                     │
│                          │  - Zone mapping     │                     │
│                          └──────────┬──────────┘                     │
│                                     │                                │
│                                     ▼                                │
│                          [ Cleaned CSV (cleaned_taxi_data.csv) ]     │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────────┐
│                       LAYER 2: ANALYTICS ENGINE                       │
├───────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌──────────────────┐         ┌──────────────────┐                  │
│  │  KPI Analyzer    │         │  SQL Analytics   │                  │
│  │   (Pandas)       │         │    (SQLite)      │                  │
│  └────────┬─────────┘         └─────────┬────────┘                  │
│           │                             │                            │
│           ▼                             ▼                            │
│  ┌──────────────────┐         ┌──────────────────┐                  │
│  │ Visualizations:  │         │  SQL Queries:    │                  │
│  │ - Monthly trends │         │ - Peak hours     │                  │
│  │ - Demand heatmap │         │ - Revenue by zone│                  │
│  │ - Fare analysis  │         │ - Top performers │                  │
│  │ - Tip patterns   │         │ - Growth metrics │                  │
│  └──────────────────┘         └──────────────────┘                  │
│           │                             │                            │
│           └──────────┬──────────────────┘                            │
│                      ▼                                               │
│           [ Analysis Results & Charts ]                              │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────────┐
│                    LAYER 3: SCALABLE PROCESSING                       │
├───────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌────────────────────────────────────────────────────┐              │
│  │           PySpark ETL Pipeline                     │              │
│  ├────────────────────────────────────────────────────┤              │
│  │                                                    │              │
│  │  ┌──────────┐    ┌──────────┐    ┌───────────┐   │              │
│  │  │  Load    │───▶│Transform │───▶│  Compute  │   │              │
│  │  │  Data    │    │  Clean   │    │   KPIs    │   │              │
│  │  └──────────┘    └──────────┘    └─────┬─────┘   │              │
│  │                                         │         │              │
│  │                                         ▼         │              │
│  │                               ┌──────────────┐   │              │
│  │                               │    Write     │   │              │
│  │                               │   Parquet    │   │              │
│  │                               └──────────────┘   │              │
│  └────────────────────────────────────────────────────┘              │
│                           │                                          │
│                           ▼                                          │
│              [ Parquet Files: Optimized for Scale ]                  │
│               - monthly_revenue.parquet                              │
│               - zone_demand.parquet                                  │
│               - peak_hour_analysis.parquet                           │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────────┐
│                    LAYER 4: GenAI INTELLIGENCE                        │
├───────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌──────────────────────────────────────────────────────┐            │
│  │        GenAI Mobility Insights Assistant             │            │
│  ├──────────────────────────────────────────────────────┤            │
│  │                                                      │            │
│  │  ┌────────────────┐         ┌─────────────────┐    │            │
│  │  │  LangChain     │────────▶│  OpenAI API     │    │            │
│  │  │  Prompt Engine │         │  (GPT-4)        │    │            │
│  │  └────────────────┘         └─────────────────┘    │            │
│  │           │                           │             │            │
│  │           ▼                           ▼             │            │
│  │  ┌────────────────┐         ┌─────────────────┐    │            │
│  │  │  KPI Context   │         │  Natural Lang   │    │            │
│  │  │  Aggregation   │         │  Query Interface│    │            │
│  │  └────────────────┘         └─────────────────┘    │            │
│  │                                                      │            │
│  └──────────────────────────────────────────────────────┘            │
│                           │                                          │
│                           ▼                                          │
│              [ Conversational Insights ]                             │
│              - "What were busiest zones?"                            │
│              - "When is surge highest?"                              │
│              - Executive summaries                                   │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────────┐
│                   LAYER 5: CLOUD DEPLOYMENT                           │
├───────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌─────────────────────────────────────────────────────┐             │
│  │              Serverless API Layer                   │             │
│  ├─────────────────────────────────────────────────────┤             │
│  │                                                     │             │
│  │  ┌──────────────┐           ┌──────────────┐      │             │
│  │  │ AWS Lambda   │           │Azure Function│      │             │
│  │  │ Functions    │           │              │      │             │
│  │  └──────┬───────┘           └──────┬───────┘      │             │
│  │         │                          │              │             │
│  │         └──────────┬───────────────┘              │             │
│  │                    ▼                              │             │
│  │         ┌─────────────────────┐                   │             │
│  │         │  API Endpoints:     │                   │             │
│  │         │  - /monthly-revenue │                   │             │
│  │         │  - /peak-hours      │                   │             │
│  │         │  - /top-zones       │                   │             │
│  │         │  - /health          │                   │             │
│  │         └─────────────────────┘                   │             │
│  └─────────────────────────────────────────────────────┘             │
│                           │                                          │
│                           ▼                                          │
│  ┌──────────────────┐    ┌────────────────┐   ┌─────────────────┐  │
│  │   S3 Storage     │    │  CloudWatch    │   │  API Gateway    │  │
│  │   (Parquet/JSON) │    │  Monitoring    │   │  Rate Limiting  │  │
│  └──────────────────┘    └────────────────┘   └─────────────────┘  │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────────┐
│                    LAYER 6: MONITORING & OPS                          │
├───────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │
│  │ CloudWatch   │  │ Application  │  │  Custom      │               │
│  │ Logs         │  │ Insights     │  │  Dashboard   │               │
│  └──────────────┘  └──────────────┘  └──────────────┘               │
│         │                  │                  │                      │
│         └──────────────────┼──────────────────┘                      │
│                            ▼                                         │
│                   ┌─────────────────┐                                │
│                   │ Alerting System │                                │
│                   │ - Errors        │                                │
│                   │ - Performance   │                                │
│                   │ - Cost          │                                │
│                   └─────────────────┘                                │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
```

---

## 🔄 Data Flow Diagram

```
┌─────────────┐
│ Raw CSV     │
│ Data (5GB)  │
└──────┬──────┘
       │
       ▼
┌──────────────────────┐
│ STEP 1: Clean Data   │
│ MobilityDataAnalyzer │
│ - Quality checks     │
│ - Type conversion    │
│ - Feature eng.       │
└──────┬───────────────┘
       │
       ├────────────────────────────────┐
       │                                │
       ▼                                ▼
┌──────────────────┐          ┌──────────────────┐
│ STEP 2: KPI      │          │ STEP 3: SQL      │
│ Compute & Viz    │          │ Analytics        │
│ - Monthly trends │          │ - GROUP BY       │
│ - Demand patterns│          │ - Window funcs   │
│ - Charts (.png)  │          │ - Aggregations   │
└──────────────────┘          └──────────────────┘
       │                                │
       └────────────┬───────────────────┘
                    │
                    ▼
          ┌─────────────────┐
          │ STEP 4: PySpark │
          │ ETL Pipeline    │
          │ - Distributed   │
          │ - Parquet out   │
          │ - Scalable      │
          └────────┬────────┘
                   │
                   ├────────────────────┐
                   │                    │
                   ▼                    ▼
          ┌─────────────────┐  ┌──────────────┐
          │ STEP 5: GenAI   │  │ STEP 6: API  │
          │ Insights        │  │ Deployment   │
          │ - Q&A           │  │ - Lambda     │
          │ - Summaries     │  │ - REST API   │
          └─────────────────┘  └──────────────┘
                   │                    │
                   └─────────┬──────────┘
                             ▼
                   ┌──────────────────┐
                   │ Business Value   │
                   │ - Actionable     │
                   │   Insights       │
                   │ - Decision       │
                   │   Support        │
                   └──────────────────┘
```

---

## ⚡ Execution Steps

### Complete End-to-End Execution

```bash
# ============================================
# FULL PIPELINE EXECUTION SCRIPT
# ============================================

#!/bin/bash

echo "🚀 Starting Urban Mobility Analytics Pipeline"
echo "=============================================="

# Step 1: Data Cleaning
echo ""
echo "📊 STEP 1: Data Cleaning"
echo "------------------------"
python src/step1_data_cleaning.py
echo "✓ Data cleaning complete"

# Step 2: KPI Analysis
echo ""
echo "📈 STEP 2: KPI Computation & Visualization"
echo "-------------------------------------------"
python src/step2_kpi_analysis.py
echo "✓ KPI analysis complete"

# Step 3: SQL Analytics
echo ""
echo "🔍 STEP 3: SQL Analytics"
echo "------------------------"
python src/step3_sql_analytics.py
echo "✓ SQL analytics complete"

# Step 4: PySpark ETL
echo ""
echo "⚙️  STEP 4: PySpark ETL Pipeline"
echo "--------------------------------"
python src/step4_pyspark_etl.py
echo "✓ PySpark ETL complete"

# Step 5: GenAI Insights
echo ""
echo "🤖 STEP 5: GenAI Insights Assistant"
echo "------------------------------------"
python src/step5_genai_assistant.py
echo "✓ GenAI insights complete"

# Step 6: Test API Locally
echo ""
echo "☁️  STEP 6: Testing Serverless API"
echo "-----------------------------------"
python src/step6_serverless_api.py
echo "✓ API testing complete"

echo ""
echo "=============================================="
echo "✅ PIPELINE EXECUTION COMPLETE!"
echo "=============================================="
echo ""
echo "Generated Outputs:"
echo "  📁 data/cleaned/cleaned_taxi_data.csv"
echo "  📊 visualizations/*.png"
echo "  💾 taxi_analytics.db"
echo "  📦 output/*.parquet"
echo "  📄 executive_summary.txt"
echo ""
echo "Next Steps:"
echo "  1. Review visualizations in 'visualizations/' folder"
echo "  2. Query SQL database: sqlite3 taxi_analytics.db"
echo "  3. Deploy API: cd deployment && sam deploy"
echo ""
```

### Individual Step Execution

#### Step 1: Data Cleaning
```bash
python src/step1_data_cleaning.py

# Expected Output:
# - cleaned_taxi_data.csv
# - Data quality report
# - Summary statistics
```

#### Step 2: KPI Analysis
```bash
python src/step2_kpi_analysis.py

# Expected Output:
# - monthly_revenue_trends.png
# - hourly_demand_heatmap.png
# - fare_distance_analysis.png
# - tip_distribution_analysis.png
```

#### Step 3: SQL Analytics
```bash
python src/step3_sql_analytics.py

# Expected Output:
# - taxi_analytics.db
# - sql_result_*.csv files
# - Query execution logs
```

#### Step 4: PySpark ETL
```bash
python src/step4_pyspark_etl.py

# Expected Output:
# - output/monthly_revenue.parquet
# - output/zone_demand.parquet
# - output/peak_hour_analysis.parquet
# - Spark execution plans
```

#### Step 5: GenAI Insights
```bash
# Set API key
export OPENAI_API_KEY="your-key-here"

python src/step5_genai_assistant.py

# Expected Output:
# - executive_summary.txt
# - Conversational Q&A responses
```

#### Step 6: Deploy API
```bash
cd deployment

# AWS
sam build && sam deploy --guided

# Azure
func azure functionapp publish taxi-analytics-api
```

---

## 📸 Screenshots Guide

### Required Screenshots for Documentation

1. **Data Cleaning Output**
   - Screenshot of terminal showing cleaning summary
   - Data quality report with before/after counts

2. **KPI Visualizations**
   - Monthly revenue trends chart
   - Hourly demand heatmap
   - Fare vs distance scatter plot

3. **SQL Query Results**
   - Peak hours query output
   - Revenue by zone results
   - Monthly growth analysis

4. **PySpark Execution**
   - Spark UI showing DAG
   - Job execution timeline
   - Parquet file outputs

5. **GenAI Interactions**
   - Q&A examples
   - Executive summary
   - Trend explanations

6. **API Deployment**
   - AWS Lambda console
   - API Gateway endpoints
   - CloudWatch logs

7. **Monitoring**
   - CloudWatch dashboard
   - Performance metrics
   - API response times

### Screenshot Locations
```
screenshots/
├── 01_data_cleaning.png
├── 02_kpi_dashboard.png
├── 03_sql_results.png
├── 04_spark_dag.png
├── 05_genai_interaction.png
├── 06_api_deployment.png
└── 07_monitoring.png
```

---

## 🐛 Troubleshooting

### Common Issues

#### 1. PySpark Java Error
```
Error: JAVA_HOME not set
```
**Solution:**
```bash
# Install Java 11
sudo apt-get install openjdk-11-jdk

# Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

#### 2. Memory Error with Large Dataset
```
MemoryError: Unable to allocate array
```
**Solution:**
```python
# Use chunked reading
chunks = pd.read_csv('large_file.csv', chunksize=10000)
for chunk in chunks:
    process(chunk)

# Or use PySpark for large files
```

#### 3. OpenAI API Rate Limit
```
RateLimitError: Rate limit exceeded
```
**Solution:**
```python
# Add retry logic with exponential backoff
import time
from openai import RateLimitError

def call_openai_with_retry(prompt, max_retries=3):
    for i in range(max_retries):
        try:
            return openai.ChatCompletion.create(...)
        except RateLimitError:
            wait_time = 2 ** i
            time.sleep(wait_time)
```

#### 4. AWS Lambda Timeout
```
Task timed out after 3.00 seconds
```
**Solution:**
```yaml
# Increase timeout in template.yaml
Properties:
  Timeout: 30  # seconds
  MemorySize: 1024  # MB
```

#### 5. Parquet Read Error
```
ParquetInvalidOrCorruptedFileException
```
**Solution:**
```python
# Verify Parquet file
import pyarrow.parquet as pq
pq.read_table('file.parquet').validate()

# Or re-write with different compression
df.write.parquet('output.parquet', compression='snappy')
```

---

## 📊 Performance Benchmarks

| Operation | Dataset Size | Time | Memory |
|-----------|--------------|------|--------|
| Data Cleaning | 1GB | 45s | 2GB |
| KPI Computation | 1GB | 30s | 1.5GB |
| SQL Analytics | 1GB | 20s | 500MB |
| PySpark ETL | 10GB | 3min | 4GB |
| GenAI Query | N/A | 2s | 100MB |

---

## 🎯 Success Criteria

Your project is complete when you have:

- ✅ Clean dataset with < 5% data loss
- ✅ 10+ visualizations showing insights
- ✅ 10+ SQL queries executed successfully
- ✅ PySpark pipeline producing Parquet outputs
- ✅ GenAI assistant answering questions
- ✅ API deployed and accessible
- ✅ Complete GitHub repository with README
- ✅ Screenshots of all execution steps
- ✅ Architecture diagram documented

---

## 🚀 Next Level Enhancements

1. **Real-Time Processing**
   - Apache Kafka for streaming
   - Real-time dashboard updates
   - Live demand prediction

2. **Machine Learning**
   - Demand forecasting (Prophet, LSTM)
   - Dynamic pricing optimization
   - Customer churn prediction

3. **Advanced Visualization**
   - Interactive Plotly dashboards
   - Streamlit web app
   - Tableau/Power BI integration

4. **Multi-City Support**
   - Compare NYC, SF, Chicago
   - Cross-city benchmarking
   - Best practices identification

---

 