# Flight Bookings Data Platform

An end-to-end data engineering project that transforms raw flight-booking data into analytics-ready datasets using Databricks, PySpark, Delta Lake, and dbt.

## Architecture

```
Source CSV files
      |
      v
Bronze  - Raw ingestion with Auto Loader
      |
      v
Silver  - Data cleaning, validation and CDC
      |
      v
Gold    - Dimensional modeling and incremental MERGE
      |
      v
dbt     - Analytics, business models and data tests
```

## Tech Stack

- Databricks & PySpark: Lakehouse development and distributed processing
- Delta Lake: Reliable storage, transactions and Change Data Feed
- Auto Loader: Incremental ingestion of source files
- Lakeflow Spark Declarative Pipelines: Declarative data transformations
- dbt: SQL analytics, data testing and documentation
- Git & GitHub: Version control

## Data Layers

### Bronze — Raw ingestion

- Incrementally ingests CSV files using Auto Loader (`cloudFiles`).
- Stores source data in Delta format.
- Handles schema evolution and rescued data.

### Silver — Cleaned and standardized data

- Casts columns, standardizes dates and applies data quality rules.
- Uses Delta Change Data Feed and Auto CDC for flights, passengers and airports.
- Applies SCD Type 1 to maintain current records.
- Bookings currently use a streaming read with validation expectations.

### Gold — Dimensional model

Creates analytics-ready tables:

- `dim_passengers`
- `dim_flights`
- `dim_airports`
- `fact_bookings`

Uses watermark-based incremental processing, surrogate keys, audit timestamps and Delta `MERGE` to insert and update dimension records. The current dimension implementation uses SCD Type 1.

### dbt — Analytics

Uses Gold tables as sources to build SQL-based analytical models, with tests such as `not_null`, `unique`, and `relationships`.

## Incremental Processing

| Layer     | Processing approach               |
| --------- | --------------------------------- |
| Bronze    | Auto Loader                       |
| Silver    | Delta CDF + Auto CDC              |
| Gold      | Timestamp watermark + Delta MERGE |
| Analytics | dbt models and tests              |

## Repository Structure

```
flight-bookings-data-platform/
├── databricks/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── flight_bookings_analytics/
│   ├── models/
│   ├── tests/
│   └── dbt_project.yml
└── README.md
```
