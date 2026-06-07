# Soda Core — Data Quality Guide

Install, run, and query **Soda Core** checks for **ecom_clickstream**. No Soda Cloud required — results go to `ecom_clickstream.dq.*`.

Pipeline & deployment: **[README.md](../README.md)** · Table schemas: **[UNITY_CATALOG_SCHEMAS.md](../UNITY_CATALOG_SCHEMAS.md)**

---

## Layout

```
soda/
├── dq_data.py          # datasets.yml + temp views
├── dq_run.py           # _run_one_scan (main entry)
├── dq_persist.py       # save results to Delta
├── install_soda.ipynb  # cluster install
├── tests_soda.ipynb    # run DQ scans
├── config/datasets.yml
└── checks/{bronze,silver,gold}/*.yml
```

**Entry point:** `soda/tests_soda.ipynb`

---

## Install

**Soda Core 3.3.22** (not `soda-spark-df` — that requires Soda Cloud).

| Where | How |
|-------|-----|
| **Local** | `pip install -r requirements.txt` then `./soda/install_local.sh` |
| **Cluster** | Run `soda/install_soda.ipynb` |

Restart the kernel after editing `soda/*.py`.

---

## Check framework

One YAML per layer + pillar: `checks/{bronze\|silver\|gold}/{pillar}.yml`

| Pillar | Covers |
|--------|--------|
| `contracts` | Schema, required keys, JSON keys in `payload` |
| `freshness_and_schema_drift` | Freshness, lineage (`_ingested_at`, `_source_file`) |
| `expectations` | Uniqueness, validity, business rules |
| `anomalies` | Volume / metric bands (`warn:` for soft alerts) |
| `integrity` | Reconciliation, FK checks |

Outcomes: `pass` · `warn` · `fail` · `error`

Prefix check names with pillar tags, e.g. `[contracts] user_id not null`.

**Bronze note:** one JSON Delta table on the Volume; `dq_data.register_views()` creates filtered temp views (`bronze_users`, etc.) from `datasets.yml` before each scan.

---

## datasets.yml

Maps Soda view names to data sources. YAML `checks for <view_name>:` must match a key here.

```yaml
sources:
  bronze_json:
    path: /Volumes/ecom_clickstream/bronze/delta/clickstream_json/

bronze:
  bronze_users:
    source: bronze_json
    entity: users

silver:
  silver_users:
    source: table
    name: ecom_clickstream.silver.users
```

---

## Python modules

| Module | Role |
|--------|------|
| `dq_data.py` | `load_datasets`, `register_views`, path helpers |
| `dq_run.py` | `_run_one_scan` — runs all layers/pillars, returns `scan_id` |
| `dq_persist.py` | `create_dq_tables`, `save_scan`, `build_rows` |

```python
from dq_data import find_soda_dir, load_datasets
from dq_run import _run_one_scan

SODA_DIR = find_soda_dir()
registry = load_datasets(SODA_DIR)
scan_id = _run_one_scan(spark, SODA_DIR, registry)
# optional: layers=["silver", "gold"]
```

---

## Run tests

1. Install Soda (local or cluster).
2. Open **`soda/tests_soda.ipynb`** → run all cells.
3. Output prints each check, then `=== BATCH <uuid> ===` with pass/fail counts.

Deploy soda files to workspace: `databricks bundle deploy -t dev`

---

## Query results

Filter by `scan_id` from the batch run.

```sql
-- Summary per layer/pillar
SELECT medallion_layer, check_category, checks_pass, checks_warn, checks_fail, checks_total
FROM ecom_clickstream.dq.scan_summary
WHERE scan_id = '<batch-uuid>'
ORDER BY medallion_layer, check_category;

-- Failures and warnings
SELECT medallion_layer, dataset_name, check_name, outcome, check_value
FROM ecom_clickstream.dq.check_results
WHERE scan_id = '<batch-uuid>' AND outcome IN ('fail', 'warn')
ORDER BY outcome DESC;
```

---

## Change checks

1. Edit `soda/checks/{layer}/{pillar}.yml` (use view names from `datasets.yml`).
2. Re-run `_run_one_scan(spark, SODA_DIR, registry)`.

---

## Quick reference

| Item | Location |
|------|----------|
| Run scans | `soda/tests_soda.ipynb` or `_run_one_scan(...)` |
| Data sources | `soda/config/datasets.yml` |
| Check YAML | `soda/checks/{bronze,silver,gold}/*.yml` |
| Results tables | `ecom_clickstream.dq.scan_summary`, `.check_results` |

- [Soda + Spark DataFrames](https://docs.soda.io/soda-documentation/soda-v3/data-source-reference/connect-spark#connect-to-spark-dataframes)
- [Soda Core install](https://docs.soda.io/soda-documentation/soda-v3/quick-start-sip/install)

---

*Soda Core 3.3.22 · catalog `ecom_clickstream`*
