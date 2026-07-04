# 🛒 Data Lakehouse Pipeline for E-commerce Analytics | PySpark, PostgreSQL, Apache Superset

> End-to-end Data Lakehouse pipeline: from raw CSV files → Bronze–Silver–Gold architecture → PostgreSQL → Apache Superset dashboards → Customer Segmentation

---

## 📚 Table of Contents

- 📖 [Project Overview](#-project-overview)
- 🏗️ [Project Architecture](#%EF%B8%8F-project-architecture)
- 🛠️ [Technology Stack](#%EF%B8%8F-technology-stack)
- 📂 [Dataset](#-dataset)
- ⚙️ [Data Pipeline](#%EF%B8%8F-data-pipeline)
- 📊 [Dashboard](#-dashboard)
- 🤖 [Customer Segmentation](#-customer-segmentation)
- 🚀 [How to Run](#-how-to-run)

---

# 📖 Project Overview

Modern e-commerce businesses generate large volumes of data from orders, customers, products, payments, and reviews. However, these datasets are often stored across multiple files and require significant processing before they can support business reporting and analytics.

This project builds an end-to-end Data Lakehouse pipeline that transforms raw e-commerce data into analytics-ready datasets using the Brazilian E-Commerce Public Dataset (Olist). The pipeline ingests raw CSV files, processes them through Bronze, Silver, and Gold layers with PySpark, stores analytical tables in PostgreSQL, visualizes business insights with Apache Superset, and applies K-Means clustering for customer segmentation.

---

# 🏗️ Project Architecture

```
Brazilian E-Commerce CSV Files
                │
                ▼
        Bronze Layer (MinIO)
                │
                ▼
      PySpark Data Processing
                │
        Bronze → Silver
                │
                ▼
      PySpark Transformations
                │
         Silver → Gold
                │
        ┌───────┴────────┐
        ▼                ▼
 PostgreSQL      Customer Features
        │                │
        ▼                ▼
 Apache Superset     K-Means Clustering
```

---

# 🛠️ Technology Stack

| Category | Technology |
|------------|------------|
| Programming | Python |
| Data Processing | PySpark 3.5 |
| Workflow | Apache Airflow |
| Object Storage | MinIO |
| Database | PostgreSQL |
| Visualization | Apache Superset |
| Machine Learning | Scikit-learn (K-Means) |
| Containerization | Docker Compose |

---

# 📂 Dataset

The project uses the **Brazilian E-Commerce Public Dataset by Olist**.

Dataset Characteristics:

- Approximately **99,000 Orders**
- Around **96,000 Customers**
- Data collected between **2016–2018**
- Multiple relational CSV files

Main tables include:

- Orders
- Order Items
- Customers
- Products
- Sellers
- Payments
- Reviews
- Geolocation

Dataset Source:

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

<img width="2486" height="1496" alt="Dataset" src="https://github.com/user-attachments/assets/99f00a65-dc99-42a2-b156-39f5052efaa6" />

---

# ⚙️ Data Pipeline

The project follows the Medallion Architecture consisting of Bronze, Silver, and Gold layers.

## 🥉 Bronze Layer

Raw CSV files are ingested into MinIO without modifying their original structure. This layer preserves the source data for traceability and future processing.

Main tasks:

- Store raw CSV files
- Preserve original schema
- Maintain raw data history

---

## 🥈 Silver Layer

PySpark performs data cleaning and standardization.

Main transformations include:

- Data type conversion
- Timestamp standardization
- Duplicate removal
- Invalid record filtering
- Basic data validation

The cleaned datasets are stored as Parquet files for efficient processing.

---

## 🥇 Gold Layer

The Gold layer generates analytics-ready tables that can be directly consumed by reporting tools.

Generated tables include:

- fact_sales
- dim_customer
- dim_product
- dim_date
- monthly_summary
- product_performance
- customer_features

---

# 📊 Dashboard

The processed Gold tables are loaded into PostgreSQL and visualized using Apache Superset.

---

## 📈 Business Overview Dashboard

<img width="999" height="701" alt="Screenshot 2026-07-05 at 00 28 02" src="https://github.com/user-attachments/assets/a02626d1-05df-435d-a4fc-164be8c2b4d0" />

### Key Insights

- Total Revenue: **20.5M**
- Total Orders: **99.4K**
- Average Order Value: **205.86**
- Unique Customers: **96.1K**

The dashboard provides an overall view of business performance through revenue trends, order volume, and order status distribution. Revenue increased steadily throughout 2017 and remained strong in 2018, while most orders were successfully delivered.

---

## 👥 Customer Analytics Dashboard

<img width="824" height="800" alt="Screenshot 2026-07-05 at 00 28 17" src="https://github.com/user-attachments/assets/47a33b5b-399b-4661-be00-fff9e5134d6c" />

### Key Insights

- Total Customers: **96.1K**
- Average Customer Spend: **213.03**
- Repeat Customer Rate: **12.43%**

Although repeat customers represent a relatively small portion of the customer base, they spend significantly more on average than one-time buyers, indicating the importance of customer retention strategies.

---

## 📦 Product Performance Dashboard

<img width="824" height="675" alt="Screenshot 2026-07-05 at 00 28 27" src="https://github.com/user-attachments/assets/b2f1331c-3c7a-4af7-871e-2e21a547f1e2" />

### Key Insights

- Product Categories: **72**
- Products: **33K**
- Product Revenue: **13.6M**
- Average Review Score: **4.04**

Categories such as **Health & Beauty**, **Watches & Gifts**, **Bed & Bath**, **Sports & Leisure**, and **Computers & Accessories** generated the highest revenue and order volume.

---

# 🤖 Customer Segmentation

In addition to business reporting, the project performs customer segmentation using the **K-Means clustering algorithm** on the `customer_features` table generated in the Gold layer.

Each customer is represented by aggregated purchasing behavior, including:

- Total Orders
- Total Spending
- Average Order Value
- Number of Purchased Products
- Average Review Score
- Cancellation Rate

The resulting customer segments help distinguish different purchasing behaviors, such as one-time buyers, loyal customers, and high-value customers. These insights complement the business dashboards by providing a customer-centric view of the processed data.

### Segmentation Results

| Cluster | Business Segment | Customers | Average Spend |
|----------|------------------|----------:|--------------:|
| 0 | Loyal Buyers | 6,375 | $569 |
| 1 | One-time Buyers | 86,756 | $142 |
| 2 | High-value Customers | 2,238 | $1,782 |
| 3 | Ultra VIP Customers | 8 | $40,559 |

### Model Performance

- **Algorithm:** K-Means
- **Number of Clusters:** 4
- **Silhouette Score:** **0.7956**

---

# 🚀 How to Run

```bash
cd docker

docker compose up -d

source ~/lakehouse-env/bin/activate

python spark_jobs/olist/bronze_to_silver.py

python spark_jobs/olist/silver_to_gold.py

python spark_jobs/olist/kmeans_clustering.py
```


