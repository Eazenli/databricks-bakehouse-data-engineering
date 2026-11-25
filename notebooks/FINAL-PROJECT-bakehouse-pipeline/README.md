## Bakehouse Sales – End-to-End Data Engineering Pipeline

### 1. Project Overview

This project implements an end-to-end data engineering pipeline using **Databricks, Apache Spark (PySpark), Delta Lake, and GitHub Actions**.

It consolidates the ingestion, transformation, data quality, incremental processing, and performance analysis techniques developed throughout the Bakehouse Data Engineering practice project.

The pipeline follows a **Medallion Architecture (Bronze, Silver, Gold)** and produces daily franchise sales metrics from the Databricks `samples.bakehouse` dataset.

#### Key Features

- Modular PySpark code organized into reusable Python modules.
- Data quality validation and rejected-record tracking.
- Incremental transaction processing with Delta Lake.
- Insert-only `MERGE` for replay-safe Silver processing.
- Execution audit with batch and run identifiers.
- Gold-layer aggregation and revenue reconciliation.
- Unit testing with `pytest` and continuous integration with GitHub Actions.
- Workflow orchestration with Databricks Lakeflow Jobs.
- Spark execution plan inspection.

### 2. Data Sources

The pipeline uses three tables from the Databricks sample dataset:

| Source Table | Role |
|---|---|
| `samples.bakehouse.sales_transactions` | Sales transaction fact data |
| `samples.bakehouse.sales_customers` | Customer reference data |
| `samples.bakehouse.sales_franchises` | Franchise reference data |

Customer and franchise dimensions are initialized as fixed reference tables. Transactions are processed incrementally using simulated batches.

### 3. Architecture

```text
                  samples.bakehouse
                         |
           +-------------+-------------+
           |             |             |
      Transactions    Customers     Franchises
           |             |             |
           v             v             v
        Bronze        Bronze        Bronze
           |             |             |
           |        Standardize    Standardize
           |         Validate       Validate
           |             |             |
           |             v             v
           |        Silver Customers  Silver Franchises
           |             |             |
           +-------------+-------------+
                         |
                Transaction Validation
                and Deduplication
                         |
                  +------+------+
                  |             |
                  v             v
            Invalid Records  Silver Transactions
                  |             |
                  v             v
             Reject Table   Delta MERGE
                                |
                                v
                         Gold Aggregation
                                |
                                v
                      Daily Franchise Sales
```

#### Bronze Layer

The Bronze layer preserves ingested source records and records execution metadata, including:

- `_batch_id`
- `_run_id`
- `_ingested_at`
- `_source`

Transaction batches are appended to Bronze, including repeated ingestion attempts.

#### Silver Layer

The Silver layer applies standardization, duplicate handling, and business validation rules.

Invalid transaction records are stored separately with rejection reasons.

Valid transactions are inserted using an insert-only Delta Lake `MERGE` on `transactionID`. Existing transactions are checked for conflicting business values before merging.

#### Gold Layer

Gold is rebuilt from the cumulative Silver transaction dataset.

The daily franchise sales dataset contains:

| Metric | Description |
|---|---|
| `sum_revenue` | Total daily revenue per franchise |
| `avg_revenue` | Average transaction revenue |
| `amount_of_customers` | Number of distinct customers |
| `rank_sum_revenue` | Daily franchise revenue ranking |

### 4. Project Structure

```text
FINAL-PROJECT-bakehouse-pipeline/
|
├── README.md
|
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── ingestion.py
│   ├── transformation.py
│   ├── quality.py
│   └── pipeline.py
|
├── tests/
│   ├── conftest.py
│   ├── test_transformations.py
│   └── test_quality.py
|
├── notebooks/
│   ├── 00_run_tests.ipynb
│   ├── 01_initialization.ipynb
│   ├── 02_run_pipeline.ipynb
│   ├── 03_business_analysis.ipynb
│   ├── 04_final_validation.ipynb
│   └── 05_performance_review.ipynb

```

#### Python Modules

| Module | Responsibility |
|---|---|
| `config.py` | Source and target table configuration |
| `ingestion.py` | Bronze ingestion and Delta table initialization |
| `transformation.py` | Data standardization, enrichment, and Gold metrics |
| `quality.py` | Data quality checks, validation, and consistency checks |
| `pipeline.py` | Batch processing, Delta operations, and execution audit |

### 5. Incremental Processing

Incremental processing is demonstrated using two simulated transaction batches divided by a date boundary.

The processing sequence is:

1. Initialize customer and franchise reference dimensions.
2. Ingest transaction Batch A into Bronze.
3. Standardize, deduplicate, and validate Batch A.
4. Insert valid transactions into Silver using Delta `MERGE`.
5. Store invalid records and rebuild Gold.
6. Repeat the processing for Batch B.
7. Replay Batch B to verify idempotence.

#### Idempotence

The replay test verifies that reprocessing an already-ingested batch does not change the resulting Silver and Gold business datasets.

The validation checks include:

- Record counts before and after replay.
- Bidirectional dataset comparison using `exceptAll`.
- Key uniqueness.
- Revenue totals before and after replay.

Bronze ingestion and audit history may record multiple execution attempts, while Silver and Gold remain logically unchanged after a successful replay.

### 6. Data Quality

Data quality checks cover:

- Missing business identifiers and required fields.
- Identical duplicate records.
- Conflicting records sharing a business key.
- Invalid transaction dates, quantities, and prices.
- Transaction amount inconsistencies.
- Unknown customer and franchise references.
- Invalid customer and franchise attributes.

Rejected records retain a `dq_reason` for investigation.

### 7. Testing and Continuous Integration

#### Unit Tests

The project uses `pytest` with synthetic PySpark DataFrames to test transformation and validation functions.

The test suite covers:

- Schema standardization and calculated fields.
- Duplicate detection and removal.
- Customer and franchise validation.
- Transaction validation and rejection reasons.
- Conflicting business keys.

The tests can be executed manually in Databricks through `00_run_tests.ipynb`.

#### GitHub Actions

A GitHub Actions workflow runs the unit tests automatically on pushes and pull requests targeting `main`.

The workflow:

1. Checks out the repository.
2. Configures Java and Python.
3. Installs PySpark and pytest.
4. Executes the unit tests with a local Spark session.

This CI process validates reusable Python logic independently of the Databricks workspace.

### 8. Databricks Job Orchestration

The pipeline is orchestrated with **Databricks Lakeflow Jobs**.

The main workflow follows this task dependency order:

```text
01_initialization
        |
        v
02_run_pipeline
        |
        v
04_final_validation
```

The Job coordinates initialization, incremental processing, and final validation.

The notebooks `03_business_analysis` and `05_performance_review` are used separately for analytical exploration and performance inspection.

The initial workflow is intended for manual execution and demonstration. A recurring production workflow would require separating one-time initialization from routine incremental processing.

### 9. Validation and Audit

The project maintains a **pipeline audit table** recording execution information such as:

- Run identifier and batch identifier.
- Start and end timestamps.
- Received, valid, and invalid row counts.
- Duplicate-removal counts.
- Execution status.

Final validation checks include:

- Silver transaction key uniqueness.
- Gold aggregation key uniqueness.
- Revenue reconciliation between Silver and Gold.
- Idempotence after replaying Batch B.

### 10. Spark Performance Review

Spark execution plans are inspected in `05_performance_review.ipynb` using:

```python
df.explain("formatted")
```

The review focuses on:

- Join strategies, including broadcast joins for small reference datasets.
- Column pruning.
- Shuffle and exchange operations.
- Aggregation and window-function execution.

The Databricks Spark UI can also be used to examine execution metrics, stages, and task performance.

