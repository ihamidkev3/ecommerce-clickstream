# ecom_clickstream

Databricks medallion pipeline for the simulated e-commerce clickstream dataset (Marketplace). Catalog: **`ecom_clickstream`**.

Bronze → Silver → Gold, with **Soda Core** DQ on all layers. Schemas: **[UNITY_CATALOG_SCHEMAS.md](UNITY_CATALOG_SCHEMAS.md)** · Soda runbook: **[soda/SODA_DATABRICKS_GUIDE.md](soda/SODA_DATABRICKS_GUIDE.md)**

---

## Layout

```
├── etl/                           # pipeline notebooks
│   ├── Bronze/2.Bronze.ipynb
│   ├── Bronze/bronze_json.py
│   ├── Silver/3..Silver.ipynb
│   ├── Gold/4. Gold.ipynb
│   └── Analytics/5. analytics.ipynb
├── soda/                          # DQ checks + runner + notebooks
│   ├── install_soda.ipynb
│   ├── tests_soda.ipynb
│   ├── dq_data.py / dq_run.py / dq_persist.py
│   └── checks/{bronze,silver,gold}/
├── databricks.yml                 # bundle sync (etl + soda)
├── requirements.txt
└── README.md
```

---

## Pipeline

| Layer | Where | Notebook |
|-------|-------|----------|
| **Bronze** | One JSON Delta table on Volume | `etl/Bronze/2.Bronze.ipynb` |
| **Silver** | `ecom_clickstream.silver.*` | `etl/Silver/3..Silver.ipynb` |
| **Gold** | `ecom_clickstream.gold.*` | `etl/Gold/4. Gold.ipynb` |
| **DQ** | `ecom_clickstream.dq.*` | `soda/tests_soda.ipynb` |

**Bronze path:** `/Volumes/ecom_clickstream/bronze/delta/clickstream_json/`  
Each row: `entity_type`, `payload` (JSON), `_ingested_at`, `_source_file`. Entities: `users`, `products`, `events`, `sales`, `item_lookup`.

**Silver:** parses `payload` per entity → typed UC tables (dedup, cast, explode nested fields).  
**Gold:** `dim_users`, `dim_products`, `fact_sales`, `fact_event`.

---

## Architecture

<img width="1856" height="683" alt="Architecture" src="https://github.com/user-attachments/assets/18d6378f-0824-4e41-973d-428786d0a3eb" />

<img width="1402" height="682" alt="Data flow" src="https://github.com/user-attachments/assets/a7ed10b6-cee1-4f7f-85ce-573c1011479f" />

<img width="1379" height="1016" alt="Schema" src="https://github.com/user-attachments/assets/8b7447a8-f422-4d78-9e43-6d691bf72d53" />

---

## Deploy & local dev

```bash
databricks bundle deploy -t dev          # sync etl/ + soda/ to workspace
```

```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt && ./soda/install_local.sh
```

---

## Data quality

15 Soda scans (5 pillars × 3 layers). Results append to `ecom_clickstream.dq.scan_summary` and `check_results`.

```python
from dq_data import find_soda_dir, load_datasets
from dq_run import _run_one_scan

registry = load_datasets(find_soda_dir())
scan_id = _run_one_scan(spark, find_soda_dir(), registry)
```

Details: **[soda/SODA_DATABRICKS_GUIDE.md](soda/SODA_DATABRICKS_GUIDE.md)**

---

*Catalog `ecom_clickstream` · Soda Core 3.3.22*
