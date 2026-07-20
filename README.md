# 🚕 Trip data pipeline for taxi app

End-to-end data engineering pipeline on Databricks that processes taxi driver satisfaction and trip data from a popular taxi app that operates in Cuenca-Ecuador, built with a medallion architecture (Bronze → Silver → Gold), Delta Live Tables, and Databricks Asset Bundles.

![SDP](https://github.com/user-attachments/assets/4a18e7ae-0497-490e-b936-6fbfa7fe90c9)
*SDP pipeline for raw ingestion along with trasnsformations in silver*

![GoldJob](https://github.com/user-attachments/assets/5b1f2ac4-4090-454a-84fd-2c39d1a0826a)

*Gold layer job with SQL tasks*

## 📌 Goal

A robust, idempotent, parameter-driven pipeline covering ingestion, transformation, and analytics consumption, deployed and versioned through CI/CD-style and TDD best practices with Databricks Asset Bundles and GitHub.

---

## 🏗️ Architecture

| Layer | Goal | Approach |
|---|---|---|
| **Bronze** | Raw ingestion from CSV | Autoloader with explicit schema enforcement + column name sanitization |
| **Silver** | Cleans, validates, and conforms data | Delta Live Tables pipeline (`Tproject_etl`) with a quarantine pattern driven by expectations loaded from a rules table |
| **Gold** | Serves analytics-ready aggregates | Lakeflow Job with SQL Tasks producing `trips_obt_gold` and `daily_metrics_gold` as plain Delta tables |
| **App** | Visualizes Gold data | Streamlit dashboard deployed via Databricks Apps, querying Gold through a SQL Warehouse |

Orchestration also includes a **file arrival trigger** job that kicks off the Silver DLT pipeline when new files land in the raw data volume, with cooldown/debounce controls to avoid redundant runs.

![Dashboard1](https://github.com/user-attachments/assets/4e94500d-4eb9-4e2c-926e-66b571ad4ff8)
![Dashboard2](https://github.com/user-attachments/assets/74d7bd54-2f7e-4e02-b000-3e6124bdf604)
*Screenshot: Streamlit dashboard showing trip KPIs, daily trends, and the detailed trip table.*

---


## ⚙️ Requirements

- Databricks account (Free Edition supported)
- Python
- VS Code
- Git
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)

---

## 🚀 Getting started

```bash
# Clone
git clone https://github.com/VladyDE/trip-data-pipeline.git
cd trip-data-pipeline

# Authenticate on databricks with ur account
databricks auth login

# Run tests locally (Databricks Connect) with uv
uv run pytest

# Deploy to dev
databricks bundle deploy --target dev
```

Catalog and schema are parameterized via bundle variables (`${var.catalog}`, `${var.schema}`); the app currently reads them from `apps/dashboard/app.yaml`.

---

## 🧪 Development approach

- **TDD:** tests are written alongside pipeline code, using `conftest.py` fixtures for the Spark session.
- **Quarantine pattern:** expectations are applied only at the point data enters a layer, splitting records into "good to go" vs. quarantined — not used to validate in-layer transformations.
- **Idempotency:** evaluated explicitly as part of the project, including how streaming tables interact with restores (see Notes below).

---

## 📝 Notes & things to keep in mind

- Streaming tables support time travel but not restore-based rollbacks — a stream treats restored data as new appends, causing duplicates. Schema evolution mistakes with Autoloader require a full refresh, not a rollback.
- Materialized Views weren't used for Gold: they provision an internal pipeline on every refresh, which is only worth the overhead when source tables have row tracking, data volume is large, and refresh frequency is high. Plain `CREATE OR REPLACE TABLE ... AS SELECT` was the better fit here.
- Gold layer aggregations are tested by running SQL directly through `spark.sql()` inside pytest.
- The app initially couldn't reach the Gold tables despite correct warehouse permissions — the real issue was passing the warehouse **ID** instead of the warehouse **HTTP path**; enabling debug logging surfaced this quickly.

