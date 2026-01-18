# End-to-End Data Engineer Project – Local Lakehouse Pipeline

## Overview
This project demonstrates a **production-style batch data pipeline** built locally using modern data engineering tools and best practices. The pipeline ingests data incrementally from a **PostgreSQL OLTP source**, stores it as **Parquet files in a raw data layer**, applies transformations using **dbt**, and orchestrates the entire workflow with **Apache Airflow**, all running in **Docker**.

The design closely mirrors real-world lakehouse architectures while remaining fully runnable on a local machine.

---

## Architecture

**Source → Raw → Staging → Analytics**

1. **Source (OLTP)**: PostgreSQL
2. **Raw Layer**: Immutable Parquet files
3. **Staging Layer**: Cleaned and deduplicated incremental data
4. **Analytics Layer**: Star schema models (facts & dimensions) in DuckDB
5. **Orchestration**: Apache Airflow (Dockerized)

---

## Tech Stack

| Category | Tools |
|------|------|
| Source Database | PostgreSQL |
| Programming | Python |
| Orchestration | Apache Airflow |
| Transformation | dbt |
| Analytical Store | DuckDB |
| Storage Format | Parquet |
| Containerization | Docker |

---

## Key Features

- Incremental extraction from PostgreSQL using timestamp-based watermarks
- Immutable raw data storage using Parquet
- Idempotent pipeline design (safe retries & re-runs)
- Lookback window handling for late-arriving data
- Star schema dimensional modeling with dbt
- Data quality tests and auto-generated documentation
- Fully local and cost-free execution

---

## Incremental Processing Strategy

- Each source table includes an `updated_at` column
- A metadata checkpoint stores the maximum processed timestamp
- On each run:
  - Only records with `updated_at > last_checkpoint` are extracted
  - A configurable lookback window allows safe reprocessing of late data
- dbt incremental models apply deterministic merges using business keys

This guarantees **idempotency** and **data correctness**.

---

## Airflow DAG

The Airflow DAG (`de_batch_pipeline`) orchestrates the following steps:

1. **Extract Postgres to Raw**
   - Incremental extraction
   - Write Parquet files to raw layer

2. **Incremental Raw to Staging**
   - Deduplication
   - Schema alignment

3. **dbt Run**
   - Build fact and dimension tables
   - Apply tests and documentation

---

## dbt Models

- Incremental models
- Star schema design
- Surrogate keys
- Tests:
  - Not null
  - Unique
  - Referential integrity

Documentation is auto-generated using dbt docs.

---

## Project Structure

```
project-root/
│
├── airflow/
│   ├── dags/
│   ├── logs/
│   └── docker-compose.yml
│
├── data/
│   ├── raw/
│   └── staging/
│
├── etl/
│   ├── extract_postgres.py
│   └── incremental_load.py
│
├── dbt/
│   ├── models/
│   ├── tests/
│   └── dbt_project.yml
│
└── warehouse/
    └── warehouse.duckdb
```

---

## How to Run Locally

### Prerequisites
- Docker Desktop
- Git

### Steps

1. Clone the repository
2. Navigate to the `airflow` directory
3. Start Airflow:

```bash
docker compose up
```

4. Open Airflow UI:

```
http://localhost:8081
```

5. Login using credentials printed in logs
6. Trigger the `de_batch_pipeline` DAG

---

## Why This Design?

- **Parquet raw layer** decouples extraction from transformation
- **dbt** enforces clean, testable, SQL-based transformations
- **Airflow** handles orchestration, not business logic
- **DuckDB** provides an efficient local analytics engine
- **Docker** ensures reproducibility

This mirrors real-world production architectures while remaining lightweight.

---

## Future Enhancements

- Replace DuckDB with Snowflake / BigQuery
- Add CDC-based ingestion
- Introduce data quality monitoring
- CI/CD for dbt models

---

## Author

**Muhammad Arshad**  
Aspiring Data Engineer

---

## Disclaimer

This project is built for learning and portfolio demonstration purposes and follows industry best practices adapted for local execution.

