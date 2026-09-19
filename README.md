# Tel Aviv Municipality Business Compensation Pipeline

## Overview

This project implements a PySpark Medallion Architecture pipeline in Databricks to calculate 2023 annual compensation for businesses affected by street closures in Tel Aviv.

## Architecture

```
Street Closures CSV (Google Cloud Storage)
        ↓
Bronze: street_bronze
        ↓
Silver: street_closures_silver
        ↓
                              Gold: annual_business_compensation_2023
        ↑                       Gold: annual_street_compensation_2023
Silver: business_silver
        ↑
Bronze: business_bronze
        ↑
Businesses JSON API (Tel Aviv ArcGIS REST)
```

The pipeline follows the **Bronze → Silver → Gold** medallion pattern:

- **Bronze**: Raw data ingestion from external sources, persisted as Delta tables with minimal transformation.
- **Silver**: Cleaning, standardization, type casting, normalization, and deduplication.
- **Gold**: Business-level transformations — compensation calculation, aggregation, and final analytics tables.

## Data Sources

- **Street Closures**: CSV file from a public Google Cloud Storage URL (`rechov_sagur.csv`) containing Tel Aviv street closure records.
- **Businesses**: JSON from the Tel Aviv ArcGIS REST API (`gisn.tel-aviv.gov.il/arcgis/rest/services/IView2/MapServer/925/query`).

## Bronze Layer

The Bronze notebook (`streets_business_bronze_ingestion`) downloads both data sources to Unity Catalog Volumes and loads them into Spark DataFrames with schema inference (all columns as strings). The raw DataFrames are persisted as Delta tables:

- `workspace.bronze.street_bronze` — raw street closure records
- `workspace.bronze.business_bronze` — raw business JSON (single row with nested features array)

## Silver Layer

### Street Silver (`street_silver_creation`)

Cleans and standardizes street closure records:

- Rename Hebrew columns to descriptive English names
- Cast data types (integers, timestamps)
- Handle zero/placeholder values as nulls
- Normalize and trim string columns
- Fill missing closure end timestamps with start timestamps
- Remove records with missing street information
- Validate closure date ranges (end >= start)

Output: `workspace.silver.street_closures_silver`

### Business Silver (`business_silver_creation`)

Cleans and normalizes business records:

- Explode and flatten nested JSON features array
- Rename columns to descriptive English names
- Cast data types (long, double, date, integer)
- Parse establishment date from Unix epoch (milliseconds)
- Normalize and trim string columns
- Remove records with no meaningful data
- Deduplicate by BusinessId (keep latest record by ImportDate)

Output: `workspace.silver.business_silver`

## Gold Layer

The Gold notebook (`compensation_model`) transforms cleaned data into compensation analytics through the following grain transformations:

```
closure (per StreetClosureId)
    → affected street (explode From/To into individual street records)
    → street/day (expand date range into individual days)
    → business/day (join businesses to closure days by street name)
    → annual business (aggregate daily compensation per BusinessId)
    → annual street (aggregate per StreetName with CostPerArea)
```

This multi-step grain expansion was necessary to guarantee that **compensation is counted only once per business per day**, even when multiple closure records affect the same street on the same day.

### Compensation Rules

```
DailyCompensation = min(Area × 100, 10,000)
```

- Only days within the 2023 calendar year are considered.
- Both streets of a closure (From and To) are treated as affected.
- A business is compensated **once per day** regardless of how many closure records affect its street that day.
- Daily compensation is capped at 10,000 NIS per business per day.
- Annual compensation is the sum of all daily compensations for that business in 2023.

### Final Outputs

| Table | Grain | Row Count |
| --- | --- | --- |
| `workspace.gold.annual_business_compensation_2023` | One row per BusinessId + StreetName | 398 |
| `workspace.gold.annual_street_compensation_2023` | One row per StreetName | 52 |

CSV exports are included in `outputs/`.

## Metrics

### Business Compensation Table

| Metric | Description |
| --- | --- |
| `CompensatedDays` | Number of days the business was affected by a closure in 2023 |
| `TotalCompensation` | Sum of daily compensations for the year |
| `CostPerArea` | TotalCompensation / Area |
| `AverageDailyCompensation` | TotalCompensation / CompensatedDays |

### Street Compensation Table

| Metric | Description |
| --- | --- |
| `AffectedBusinesses` | Count of distinct businesses affected on this street |
| `TotalBusinessArea` | Sum of business areas on this street |
| `CompensatedBusinessDays` | Total business-days compensated on this street |
| `TotalCompensation` | Sum of all business compensations on this street |
| `CostPerArea` | TotalCompensation / TotalBusinessArea |
| `AverageCompensationPerBusiness` | TotalCompensation / AffectedBusinesses |

## Databricks Job

The pipeline is orchestrated as a Databricks Job with the following task dependency DAG:

```
Bronze ingestion
      ↓
Street Silver ──┐
                 ├── Gold Compensation
Business Silver ┘
```

- **Bronze ingestion** runs first and must complete successfully.
- **Street Silver** and **Business Silver** run in parallel after Bronze.
- **Gold Compensation** runs after both Silver tasks complete.

The pipeline has been successfully executed end-to-end as a Databricks Job.

## How to Run

1. **Prerequisites**: A Databricks workspace with Unity Catalog enabled.
2. **Execution order**: Run notebooks in dependency order: Bronze → Silver (Street + Business) → Gold. The Databricks Job handles this automatically.
3. **Job trigger**: Navigate to the Databricks Job and click "Run Now" for a one-time execution, or let the daily schedule trigger it.
4. **Outputs**: After completion, query `workspace.gold.annual_business_compensation_2023` and `workspace.gold.annual_street_compensation_2023` for results.

## Project Structure

```
tel-aviv-compensation-pipeline/
├── README.md
├── bronze/
│   └── streets_business_bronze_ingestion.py
├── silver/
│   ├── business_silver_creation.py
│   └── street_silver_creation.py
├── gold/
│   └── compensation_model.py
├── outputs/
│   ├── annual_business_compensation_2023.csv
│   └── annual_street_compensation_2023.csv
├── docs/
│   ├── job_dag.png
│   └── successful_job_run.png
└── .gitignore
```

> **Note**: `docs/job_dag.png` and `docs/successful_job_run.png` should be manually added with screenshots from the Databricks Job UI.