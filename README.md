# GCP Retail Data Pipeline using PySpark, BigQuery, and DBT

An end-to-end data engineering project that processes retail data using:
- **GCS** for raw CSV ingestion
- **Dataproc (PySpark)** for data cleansing
- **BigQuery** for storage and querying
- **DBT** for transformation, testing, and snapshotting
- **Airflow (Astro CLI)** for orchestration

## Input data
The dataset used in this project is the **[Online Retail Dataset (UCI Repository)](https://archive.ics.uci.edu/ml/datasets/online+retail)**.

## Architecture Overview
![Data Pipeline img](https://github.com/user-attachments/assets/379d2d79-5cea-4e0a-a710-5dcd724e5dda)



## Workflow
1. Upload raw retail data to GCS
2. Cleanse using PySpark job on Dataproc
3. Load cleansed data into BigQuery
4. Trigger DBT models (with snapshots & tests)
5. Monitor pipeline via Airflow

## Key Features
- Dimensional modeling with SCD Type 2 using DBT snapshots
- Automated DBT testing for data quality
- End-to-end orchestration via Airflow

## How to Run
```bash
astro dev start
# Upload CSV to GCS and trigger DAG from Airflow UI
