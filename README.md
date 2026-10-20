# Olist E-Commerce Lakehouse on Databricks

An end-to-end **medallion architecture (Bronze → Silver → Gold)** data pipeline built on **Databricks**, **PySpark**, **Delta Lake**, and **Unity Catalog**, transforming the [Olist Brazilian e-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) into a star-schema analytics-ready Lakehouse.

The pipeline ingests 9 raw CSV datasets, simulates incremental batch loads, cleans and conforms the data with **SCD Type 2** versioning and **surrogate keys**, and builds a **star schema** with a central sales fact table and aggregated business marts (daily sales, seller performance, customer LTV).

---

## Architecture

```
Source Files (CSV)
        │
        ▼
UC Volume: bronze.raw_files/staging          (batch_files notebook)
        │  split into batch_1 / batch_2 / batch_3
        ▼
Bronze Layer  ── bronze schema       (01_bronze_ingestion)
        │  raw CSV → Delta, audit metadata appended
        ▼
Silver Layer  ── silver schema       (02_silver_* notebooks)
        │  cleaning, dedup, SCD2 dims, surrogate keys,
        │  SHA-256 change detection, MERGE upserts
        ▼
Gold Layer     ── gold schema         (03_gold_* notebooks)
           Star schema fact table + analytics marts
```

All tables live in the Unity Catalog catalog **`ecommerce_lakehouse`** with schemas `bronze`, `silver`, and `gold`.

---

## Project Structure

```
olist-databricks-lakehouse/
│
├── env_steup/                       # Creates catalog, bronze/silver/gold schemas, raw_files volume
├── utils.py                         # Shared helpers: text cleaning, surrogate key generation
├── batch_files/                     # Splits staging CSVs into simulated incremental batches
├── 01_bronze_ingestion/             # Batch-parameterized raw CSV → Delta ingestion
├── pipeline.yaml                    # Orchestration job definition (Bronze → Silver → Gold DAG)
│
├── silver/
│   ├── 02_silver_customers/         # Customer dimension (SCD Type 2)
│   ├── 02_silver_orders/            # Orders (SCD1 upsert via row-hash MERGE)
│   ├── 02_silver_order_items/       # Order items
│   ├── 02_silver_order_payments/    # Payments
│   ├── 02_silver_order_reviews/     # Reviews
│   ├── 02_silver_products/          # Product dimension
│   ├── 02_silver_sellers/           # Seller dimension
│   └── 02_silver_geolocation/       # Geolocation
│
├── gold/
│   ├── 03_gold_fact_sales/          # Central fact table (star schema)
│   ├── 03_gold_daily_sales_summary/ # Daily revenue/ volume mart
│   ├── 03_gold_seller_performance/   # Seller KPI mart
│   └── 03_gold_customer_ltv/         # Customer lifetime value mart
│
├── Source Files/                    # Original Olist CSV datasets
├── LICENSE                          # MIT
└── README.md
```

---

## Pipeline Stages

### 1. Environment Setup — `env_steup`
Creates the `ecommerce_lakehouse` catalog, the `bronze`, `silver`, and `gold` schemas (with descriptive comments), and the `bronze.raw_files` volume used to land raw files.

### 2. Batch Simulation — `batch_files`
Simulates realistic incremental loading:
* Reference/dimension tables (customers, geolocation, products, sellers, category translation) go entirely into **`batch_1`**.
* Transactional fact tables (orders, order items, payments, reviews) are randomly split into **`batch_1` (70%) / `batch_2` (20%) / `batch_3` (10%)**; batch_3 also applies a simulated status update (`processing` → `delivered`) to exercise change detection.

### 3. Bronze Ingestion — `01_bronze_ingestion`
Parameterized by the `batch_name` widget. Reads each dataset's CSVs from the UC volume, appends audit metadata (`_ingested_at`, `_source_file`, `_batch_id`), and writes Delta tables (`overwrite` for the initial batch, `append` for later batches).

### 4. Silver Transformations — `silver/02_silver_*`
Per-batch cleaning and conformance:
* **Data hygiene** — trimming, casing, timestamp casting, deduplication.
* **Surrogate keys** — incremental prefixed keys (e.g., `C000001`, `S000001`) that account for the target table's existing max key (`utils.generate_surrogate_key`).
* **SCD Type 2** on the customer dimension — `is_current`, `effective_start_date` / `effective_end_date`; changed rows are expired via MERGE and new versions inserted.
* **SCD Type 1 upserts** on facts — SHA-256 `row_hash` change detection drives Delta `MERGE` (update only when the hash differs), with `_etl_inserted_at`, `_etl_updated_at`, and `_delete_flag` audit columns.

### 5. Gold Star Schema — `gold/03_gold_*`
* **`gold_fact_sales`** — central fact table joining order items, orders, products, sellers, and aggregated (split-payment-aware) payments; includes degenerate date dimensions (`purchase_year`, `purchase_month`, `purchase_day_of_week`), financial metrics (`price`, `freight_value`, `total_item_amount`, `total_payment_value`), and logistics metrics (`is_delayed`, `fulfillment_days`).
* **`gold_daily_sales_summary`** — daily revenue and order volume.
* **`gold_seller_performance`** — seller-level KPIs.
* **`gold_customer_ltv`** — customer lifetime value analytics.

---

## Orchestration — `pipeline.yaml`

The incremental pipeline is defined as a job resource (`olist_ecommerce_incremental_pipeline`) that orchestrates the Bronze → Silver → Gold flow as a task DAG, parameterized with `batch_name` (default: `batch_3`) and run queuing enabled:

```
run_bronze_ingestion
        │
        ├──► run_silver_orders ──────┐
        ├──► run_silver_order_items ─┤
        ├──► run_silver_order_payments ┤  (run in parallel)
        └──► run_silver_order_reviews ┘
                 │
                 ▼
        run_gold_fact_sales
                 │
                 ├──► run_gold_customer_ltv
                 ├──► run_gold_daily_summary
                 └──► run_gold_seller_perf
```

Instead of running notebooks manually per batch, this job executes the full incremental flow for any batch in a single trigger — Bronze ingests the batch, the Silver fact tables upsert it, and the Gold fact table and analytics marts rebuild.

---

## How to Run

Execute the notebooks in this order (each Silver/Gold notebook is parameterized with a `batch_name` widget — run once per batch):

1. **`env_steup`** — one-time: create catalog, schemas, and volume.
2. **`batch_files`** — split source CSVs into `batch_1`, `batch_2`, `batch_3` under the staging volume.
3. **`01_bronze_ingestion`** with `batch_name = batch_1` (initial full load), then repeat with `batch_2`, `batch_3` (appends).
4. **`silver/02_silver_*`** notebooks (customers first, then the rest) with `batch_name = batch_1`; repeat per batch.
5. **`gold/03_gold_*`** notebooks to build the fact table and marts.

> Note: compute runs on Databricks serverless — no cluster configuration required.

---

## Key Features

* Medallion architecture with Unity Catalog governance across all three layers
* Incremental batch processing simulated via a 3-batch split (70/20/10) with in-flight status updates
* Delta Lake `MERGE`-based incremental loads with SHA-256 row-hash change detection
* SCD Type 2 customer dimension with effective dating
* Incremental surrogate key generation safe across batch runs
* Full audit lineage: `_ingested_at`, `_source_file`, `_batch_id`, `_etl_inserted_at`, `_etl_updated_at`, `_delete_flag`
* Star schema with conformed dimensions and derived analytical metrics

## Tech Stack

* **Databricks** (serverless compute)
* **PySpark** & **Spark SQL**
* **Delta Lake** (MERGE upserts, ACID transactions)
* **Unity Catalog** (catalog/schemas/volumes governance)

## License

[MIT](LICENSE) © 2026 Yuvraj Singh