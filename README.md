# Spotify Data Engineering Project

## Overview

This project demonstrates an end-to-end Azure Data Engineering pipeline built using the Spotify streaming dataset. The pipeline automates data ingestion, transformation, and storage using Azure services while following the Medallion Architecture (Bronze, Silver, and Gold).

It supports both initial and incremental data loading and uses PySpark to transform raw data into analytics-ready datasets.

---

## Project Architecture

```text
Azure SQL Database
        │
        ▼
Azure Data Factory
(Initial & Incremental Load)
        │
        ▼
Azure Data Lake Storage Gen2
Bronze Layer (Parquet)
        │
        ▼
Azure Databricks
(PySpark Transformations)
        │
        ▼
Silver Layer (Parquet)
        │
        ▼
Gold Layer (Delta Lake)
        │
        ▼
Analytics & Reporting
```

---

## Tech Stack

- Azure SQL Database
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- PySpark
- Delta Lake
- Parquet
- Jinja2
- Databricks Asset Bundles
- Git & GitHub

---

## Key Features

- Built an end-to-end Azure Data Engineering pipeline.
- Implemented initial and incremental data loading.
- Automated data ingestion using Azure Data Factory.
- Processed and transformed data using PySpark.
- Implemented the Bronze, Silver, and Gold architecture.
- Stored Silver layer data in Parquet and Gold layer data in Delta Lake.
- Used Jinja2 templates for metadata-driven transformations.
- Managed the Databricks project using Asset Bundles.

---

## Project Workflow

1. Data is ingested from **Azure SQL Database** using **Azure Data Factory**.
2. Raw data is stored in the **Bronze** layer in Parquet format.
3. PySpark notebooks clean and transform the data into the **Silver** layer.
4. Business-ready tables are created in the **Gold** layer using Delta Lake.
5. The final data is prepared for reporting and analytics.

---

## Screenshots

The repository contains screenshots demonstrating different stages of the project, including:

- Azure Data Factory Pipelines
- Azure Data Lake Storage Gen2
- Azure Databricks Workspace
- Incremental Pipeline Execution
- Databricks Asset Bundle Deployment

---

## Learning Outcomes

Through this project, I gained hands-on experience in designing Azure-based ETL pipelines, implementing incremental data loading, working with Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, PySpark, Delta Lake, metadata-driven transformations using Jinja2, and Databricks Asset Bundles.

---

## Author

**Kanishka Verma**

B.Tech, Computer Science Engineering (AI & ML)
