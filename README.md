# StreamFlix OTT Analytics — End-to-End Data Pipeline
 
An end-to-end analytics pipeline for a Netflix-style OTT platform, built across
**SQL Server**, **Pandas**, and **PySpark** — covering data modeling, ETL, and
KPI generation on 8,800+ titles and 175,000+ synthetic user/behavioral records.
 
## What it covers
 
- **Content & engagement**: top-watched titles, popular genres, monthly viewing trends
- **Revenue**: subscription revenue by plan (Basic/Standard/Premium)
- **Users**: active cities, device usage, age group distribution
- **Retention**: churn-risk scoring based on recency of activity
- **Recommendations**: per-user top-genre ranking dataset
 
## Tech stack
 
| Layer | Tools |
|---|---|
| Relational DB | SQL Server (T-SQL: joins, CTEs, window functions, views) |
| EDA | Python, Pandas, Matplotlib |
| Big data ETL | PySpark (DataFrames, Spark SQL, window functions, partitioned Parquet) |
 
## Data
 
- `data/movies.csv` — 8,807 titles from the public Kaggle Netflix dataset (cleaned)
- `data/users.csv`, `subscriptions.csv`, `subscription_plans.csv`,
  `watch_history.csv`, `ratings.csv` — synthetic data generated to model
  realistic user behavior (6,000 users, 120,000 watch events, ~35,000 ratings)
 
## Repository structure
 
data/           6 source CSVs
sql/            SQL Server schema + KPI queries (streamflix_sql_server.sql)
pyspark/        PySpark ETL pipeline (script + Colab notebook)
pandas_eda/     Pandas EDA (script + Colab notebook + charts/)
 
## How to run
 
**SQL Server**: open `sql/streamflix_sql_server.sql` in SSMS, update the
`BULK INSERT` file paths to point at your local copy of `data/`, execute.
 
**PySpark**: run `pyspark/pyspark_etl.py` locally (`pip install pyspark`) or
open `pyspark/StreamFlix_PySpark_ETL.ipynb` in Google Colab and upload `data/`
when prompted. Produces partitioned Parquet output (not committed to this repo
— regenerate it by running the pipeline).
 
**Pandas EDA**: run `pandas_eda/phase2_pandas_eda.py` locally, or open
`pandas_eda/StreamFlix_Pandas_EDA.ipynb` in Colab. Outputs summary reports and
the charts in `pandas_eda/charts/`.
 
## Sample results
 
- Top-rated content, genre popularity, and revenue split by plan
- Churn-risk segmentation (Low / Medium / High) based on watch recency
- Monthly viewing trend across the full dataset timeframe
 
See `pandas_eda/charts/` for visualizations.
 
