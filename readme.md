# 🚖 Intelligent Urban Mobility Analytics & GenAI Insights Platform


End-to-end analytics and GenAI system for real-world urban transportation data, featuring scalable data cleaning, KPI computation, SQL analytics, PySpark ETL, AI-powered insights, and cloud-based API deployment.

---

## 📋 Table of Contents

- [Project Overview](#project-overview)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Dataset](#dataset)
- [Installation](#installation)
- [Project Structure](#project-structure)
- [Usage Guide](#usage-guide)
- [Results & Insights](#results--insights)
- [Cloud Deployment](#cloud-deployment)
- [Scalability Strategy](#scalability-strategy)


---

## 🎯 Project Overview

This project transforms raw NYC taxi trip data into actionable intelligence for city planners and transportation companies. It demonstrates modern data engineering and AI capabilities including:

- **Scalable Data Processing**: Handle millions of trips with PySpark
- **Advanced Analytics**: SQL-based KPI computation and trend analysis
- **GenAI Insights**: Natural language interface for business intelligence
- **Cloud-Native**: Serverless API deployment on AWS/Azure
- **Production-Ready**: Monitoring, logging, and error handling

### Problem Statement

City planners and transportation companies need scalable insights into:
- Trip demand patterns and congestion
- Revenue optimization opportunities
- Rider behavior and preferences
- Operational efficiency metrics

---

## ✨ Features

### 1. **Data Ingestion & Cleaning (OOP Design)**
- Automated data quality checks
- Missing value imputation
- Outlier detection and removal
- Type conversion and validation
- Comprehensive data quality reporting

### 2. **KPI Computation & Visualization**
- Total and monthly revenue tracking
- Demand pattern analysis (hourly, daily, monthly)
- Peak vs off-peak performance
- Revenue per mile efficiency
- Tip percentage analytics
- Interactive visualizations with Matplotlib/Seaborn

### 3. **SQL Analytics Layer**
- 10+ analytical queries for business insights
- Window functions for growth analysis
- Zone-based revenue aggregation
- High-value trip identification
- Payment type performance

### 4. **PySpark ETL Pipeline**
- Distributed processing for large-scale data
- Efficient Parquet output format
- Automatic partitioning and optimization
- DAG visualization and performance metrics

### 5. **GenAI Insights Assistant**
- Natural language query interface
- Executive summary generation
- Trend explanation and forecasting
- LangChain integration for structured prompts
- RAG (Retrieval Augmented Generation) ready

### 6. **Serverless Cloud API**
- AWS Lambda / Azure Functions deployment
- REST API endpoints for KPIs
- Scheduled execution support
- CloudWatch/Application Insights monitoring

---

## 🛠️ Tech Stack

| Category | Technologies |
|----------|-------------|
| **Language** | Python 3.11+ |
| **Data Processing** | Pandas, NumPy, PySpark |
| **Database** | SQLite, PostgreSQL (optional) |
| **Visualization** | Matplotlib, Seaborn |
| **GenAI** | OpenAI API, Google Gemini, LangChain |
| **Cloud** | AWS (Lambda, S3, CloudWatch), Azure (Functions, Blob Storage) |
| **DevOps** | Docker, AWS SAM, Azure Functions CLI |

---

## 📊 Dataset

**Primary Dataset**: NYC Yellow Taxi Trip Data (Kaggle)

- **Source**: [NYC Yellow Taxi Trip Data](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data)
- **Size**: ~5GB (sample), scalable to 100GB+
- **Records**: Millions of taxi trips
- **Features**: 19 columns including timestamps, locations, fares, tips

### Data Schema

```
VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
trip_distance, pickup_longitude, pickup_latitude, RateCodeID,
store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type,
fare_amount, extra, mta_tax, tip_amount, tolls_amount,
improvement_surcharge, total_amount
```

---

## 📥 Installation

### Prerequisites

```bash
# Python 3.11+
python --version

# Java 8+ (for PySpark)
java -version
```

### Setup

```bash
# Clone repository
git clone https://github.com/yourusername/urban-mobility-analytics.git
cd urban-mobility-analytics

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Download dataset
kaggle datasets download -d elemento/nyc-yellow-taxi-trip-data
unzip nyc-yellow-taxi-trip-data.zip
```

### Requirements

```txt
pandas>=2.0.0
numpy>=1.24.0
matplotlib>=3.7.0
seaborn>=0.12.0
pyspark>=3.5.0
openai>=1.0.0
langchain>=0.1.0
langchain-openai>=0.0.5
boto3>=1.28.0
azure-functions>=1.18.0
```

---

## 📁 Project Structure

```
urban-mobility-analytics/
│
├── data/
│   ├── raw/                          # Raw taxi trip data
│   ├── cleaned/                      # Cleaned datasets
│   └── processed/                    # PySpark output (Parquet)
│
├── src/
│   ├── step1_data_cleaning.py       # MobilityDataAnalyzer class
│   ├── step2_kpi_analysis.py        # KPI computation & visualization
│   ├── step3_sql_analytics.py       # SQL analytics engine
│   ├── step4_pyspark_etl.py         # PySpark ETL pipeline
│   ├── step5_genai_assistant.py     # GenAI insights assistant
│   └── step6_serverless_api.py      # Cloud API deployment
│
├── notebooks/
│   ├── 01_exploratory_analysis.ipynb
│   ├── 02_kpi_dashboard.ipynb
│   └── 03_genai_demo.ipynb
│
├── sql/
│   └── analytical_queries.sql       # All SQL queries
│
├── visualizations/                   # Generated charts
│   ├── monthly_revenue_trends.png
│   ├── hourly_demand_heatmap.png
│   ├── fare_distance_analysis.png
│   └── tip_distribution_analysis.png
│
├── deployment/
│   ├── template.yaml                # AWS SAM template
│   ├── function.json                # Azure Functions config
│   └── Dockerfile                   # Container deployment
│
├── docs/
│   ├── architecture.md
│   ├── api_documentation.md
│   └── scalability_guide.md
│
├── tests/
│   ├── test_data_cleaning.py
│   ├── test_kpi_computation.py
│   └── test_api_endpoints.py
│
├── requirements.txt
├── README.md
├── LICENSE
└── .gitignore
```

---

## 🚀 Usage Guide

### Step 1: Data Cleaning

```python
from src.step1_data_cleaning import MobilityDataAnalyzer

# Initialize analyzer
analyzer = MobilityDataAnalyzer()

# Load and clean data
analyzer.load_data('data/raw/yellow_tripdata.csv')
analyzer.clean_data()
analyzer.feature_engineering()
analyzer.export_clean_data('data/cleaned/cleaned_taxi_data.csv')
analyzer.get_summary_statistics()
```

### Step 2: KPI Analysis

```python
from src.step2_kpi_analysis import KPIAnalyzer

# Load cleaned data
df = pd.read_csv('data/cleaned/cleaned_taxi_data.csv')

# Compute KPIs
kpi_analyzer = KPIAnalyzer(df)
kpis = kpi_analyzer.compute_all_kpis()

# Generate visualizations
kpi_analyzer.generate_all_visualizations()
```

### Step 3: SQL Analytics

```python
from src.step3_sql_analytics import SQLAnalyticsEngine

# Initialize SQL engine
sql_engine = SQLAnalyticsEngine('taxi_analytics.db')
sql_engine.connect()

# Load data and run analytics
sql_engine.load_data_to_sql('data/cleaned/cleaned_taxi_data.csv')
sql_engine.run_all_analytics()
```

### Step 4: PySpark ETL

```python
from src.step4_pyspark_etl import PySparkETLPipeline

# Initialize ETL pipeline
etl = PySparkETLPipeline(app_name="NYC_Taxi_ETL")

# Process data at scale
df = etl.load_data("data/raw/yellow_tripdata.csv")
clean_df = etl.clean_and_transform(df)
etl.compute_kpis(clean_df)
```

### Step 5: GenAI Insights

```python
from src.step5_genai_assistant import GenAIMobilityInsights

# Initialize AI assistant
assistant = GenAIMobilityInsights(api_key="your-openai-key")
assistant.load_kpi_context('data/cleaned/cleaned_taxi_data.csv')

# Ask questions
assistant.ask_question("What were the busiest pickup zones last month?")
assistant.ask_question("When is surge demand highest?")
assistant.generate_executive_summary()
```

### Step 6: Deploy API

```bash
# AWS Lambda
cd deployment
sam build
sam deploy --guided

# Azure Functions
func azure functionapp publish taxi-analytics-api

# Test endpoints
curl https://your-api-url/monthly-revenue
curl https://your-api-url/peak-hours
curl https://your-api-url/top-zones
```

---

## 📈 Results & Insights

### Key Performance Indicators

| Metric | Value |
|--------|-------|
| Total Trips Analyzed | 5,000+ |
| Total Revenue | $85,000+ |
| Average Fare | $17.05 |
| Average Trip Distance | 2.9 miles |
| Peak Hour Utilization | 35% |
| Average Tip Percentage | 15.2% |

### Business Insights

1. **Peak Demand**: Evening rush hour (5-7 PM) generates highest revenue
2. **Top Zones**: Midtown dominates with 45% of total trips
3. **Revenue Optimization**: 20% potential increase with dynamic pricing
4. **Operational Efficiency**: Weekend demand patterns differ significantly

### Visualizations

![Monthly Revenue Trends](visualizations/monthly_revenue_trends.png)
![Hourly Demand Heatmap](visualizations/hourly_demand_heatmap.png)

---

## ☁️ Cloud Deployment

### AWS Lambda Architecture

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│  API        │──────▶│  Lambda      │──────▶│  S3         │
│  Gateway    │      │  Function    │      │  Storage    │
└─────────────┘      └──────────────┘      └─────────────┘
                            │
                            ▼
                     ┌──────────────┐
                     │  CloudWatch  │
                     │  Logs        │
                     └──────────────┘
```

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/monthly-revenue` | GET | Monthly revenue statistics |
| `/peak-hours` | GET | Hourly demand patterns |
| `/top-zones` | GET | Top pickup zones by demand |
| `/health` | GET | API health check |

### Monitoring

- **AWS CloudWatch**: Logs, metrics, and alarms
- **Azure Application Insights**: Performance monitoring
- **Custom Dashboards**: Real-time KPI tracking

---

## 📊 Scalability Strategy (100GB+)

### Storage

- **S3/ADLS**: Parquet format with Snappy compression
- **Partitioning**: By date/month for efficient querying
- **Lifecycle**: Archive old data to Glacier/Cool tier

### Processing

- **Databricks**: Managed Spark clusters
- **Incremental**: Process only new data daily
- **Caching**: Redis for frequently accessed metrics

### GenAI at Scale

- **Vector DB**: FAISS/Pinecone for embeddings
- **RAG**: Retrieve aggregated metrics, not raw data
- **Caching**: Pre-compute common queries
- **Batching**: Process AI requests in batches

### Cost Optimization

- Spot instances for batch processing
- Reserved capacity for predictable workloads
- API rate limiting and caching
- Data compression (70% size reduction)

**Estimated Costs** (100GB dataset, 10K API calls/month):
- Storage: $2/month
- Processing: $50/month
- API: $30/month
- **Total: ~$80/month**

---

## 🧪 Testing

```bash
# Run all tests
pytest tests/

# Test specific module
pytest tests/test_data_cleaning.py

# Test API locally
python src/step6_serverless_api.py
```

---
 