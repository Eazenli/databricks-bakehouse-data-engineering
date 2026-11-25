## Bakehouse Data Engineering Practice Project

### 1. Project Overview

This is a hands-on data engineering project built in **Databricks**, using the sample dataset _bakehouse_.

The main objective is to maintain and strengthen my skills in **SQL, Apache Spark (particularly PySpark), and data engineering** while actively looking for a new job.

The project follows a progressive, practice-oriented approach to designing and implementing a data pipeline in Databricks. I explore data ingestion, transformation, data quality, and analytical modeling through these practical exercises.

### 2. Source Dataset

The `samples.bakehouse` schema contains 6 source tables:

| Source Table | Description |
|---|---|
| `samples.bakehouse.sales_transactions` | Sales transaction facts, including dates, products, quantities, prices, customers, and franchises. |
| `samples.bakehouse.sales_customers` | Customer dimension data, including customer attributes and geographical information. |
| `samples.bakehouse.sales_franchises` | Franchise dimension data, including relationships between franchises and suppliers. |
| `samples.bakehouse.sales_suppliers` | Supplier dimension data, including supplier attributes and geographical information. |
| `samples.bakehouse.media_customer_reviews` | Raw customer reviews used to practice processing semi-structured data. |
| `samples.bakehouse.media_gold_reviews_chunked` | Preprocessed and chunked customer reviews used in advanced exercises. |

In the early data disconvery, the 6 sources table are checked, but during the data pipeline construction, only 3 tables are concerned: `samples.bakehouse.sales_transactions`, `samples.bakehouse.sales_customers` and `samples.bakehouse.sales_franchises`.

### 3. Workspace Organization

A dedicated catalog, `bakehouse_training`, has been created in my Databricks workspace to organize the tables and other data assets generated throughout the project.

#### Table Naming Conventions

The project adopts a **Medallion Architecture**, with three main data layers: Bronze, Silver, and Gold.

| Layer | Naming Convention | Purpose |
|---|---|---|
| Bronze | `bronze_<entity>` | Store source data with minimal transformations. |
| Silver | `silver_<entity>` | Store cleaned, validated, and standardized data. |
| Silver – Rejected Records | `silver_invalid_<entity>` | Store records that fail defined data quality checks for audit and further investigation. |
| Gold | `gold_<business_metric>` | Store business-oriented datasets and aggregated metrics for analytics. |

### 4. Learning Path

The project is developed incrementally through hands-on exercises in **Databricks notebooks**, using **SQL** and **PySpark**. The exercises are generally organized into 5 stages:

#### Stage 1 – Data Discovery and Profiling

- Explore the source datasets to understand their schemas, structure, and content.
- Perform initial data profiling and data quality assessments.
- Identify potential issues, including missing values, duplicates, and inconsistent data types.

#### Stage 2 – Relational Data Modeling and Analytical Exploration

- Examine relationships between fact and dimension tables.
- Validate referential integrity using joins, particularly `LEFT ANTI JOIN` and `LEFT SEMI JOIN`.
- Practice analytical SQL queries, including window functions for ranking and some Top-N analysis (e.g., Top 3 products by sales).

#### Stage 3 – Data Transformation and Quality Validation

- Develop reusable functions for data standardisation, transformation, and validation.
- Identify invalid records and preserve rejection reasons for traceability.
- Verify data uniqueness, consistency, and alignment across related datasets.
- Separate valid and rejected records into their respective Silver-layer tables.

#### Stage 4 – Incremental Processing with Delta and Performance Analysis

- Implement **incremental** data processing using Delta Lake *MERGE*.
- Apply replay-safe processing techniques to prevent duplicate records when pipelines are rerun.
- Inspect Spark execution plans to understand query execution and identify potential optimizations.

#### Stage 5 – Final Project Review and Refactoring (for more details, please read README.md in FINAL_PROJECT)

- Consolidate and refactor the PySpark ingestion, transformation, data quality, and pipeline logic into reusable Python modules, and organize the project into a maintainable structure.
- Implement unit tests with pytest to validate data transformations, deduplication, and data quality rules and set up continuous integration (CI) with GitHub Actions to execute PySpark unit tests automatically.
- Orchestrate the end-to-end data pipeline using Databricks Lakeflow Jobs.
- Validate incremental processing, Delta Lake MERGE idempotence, and Silver-to-Gold data consistency.




