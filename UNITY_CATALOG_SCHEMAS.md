# Unity Catalog Schemas — ecom_clickstream

Column reference for **`ecom_clickstream`**. Bronze lives on a Volume (not UC tables). Silver, gold, and dq are managed Delta tables.

Soda checks & temp views: **[soda/SODA_DATABRICKS_GUIDE.md](soda/SODA_DATABRICKS_GUIDE.md)**

---

## Overview

| Schema | Objects |
|--------|---------|
| `bronze` | Volume only — `/Volumes/ecom_clickstream/bronze/delta/clickstream_json/` |
| `silver` | `users`, `products`, `events`, `sales` |
| `gold` | `dim_users`, `dim_products`, `fact_sales`, `fact_event` |
| `dq` | `scan_summary`, `check_results` |

**Bronze row shape:** `entity_type`, `payload` (JSON string), `_ingested_at`, `_source_file`

| `entity_type` | ~Rows | Silver table |
|---------------|------:|--------------|
| `users` | 751k | `silver.users` → `gold.dim_users` |
| `products` | 12 | `silver.products` → `gold.dim_products` |
| `events` | 986k | `silver.events` → `gold.fact_event` |
| `sales` | 21k | `silver.sales` → `gold.fact_sales` |
| `item_lookup` | 12 | *(bronze only)* |

---

## Bronze — `clickstream_json`

| Column | Type |
|--------|------|
| `entity_type` | string |
| `payload` | string |
| `_ingested_at` | timestamp |
| `_source_file` | string |

Parsed fields live inside `payload` as JSON. Silver reads by `entity_type`. Notable raw quirks: sparse `email` on users (~84% null); sales has both `transactions_timestamp` and `transaction_timestamp` (coalesced in silver).

---

## Silver

### `silver.users` (~727k)

| Column | Type |
|--------|------|
| `user_id` | string |
| `user_first_touch_timestamp` | timestamp |
| `email` | string |

### `silver.products` (12)

| Column | Type |
|--------|------|
| `item_id`, `name` | string |
| `price` | double |

### `silver.events` (~990k)

| Column | Type |
|--------|------|
| `user_id`, `device`, `event_name`, `traffic_source` | string |
| `event_timestamp`, `event_previous_timestamp`, `user_first_touch_timestamp` | timestamp |
| `city`, `state` | string |
| `purchase_revenue_in_usd`, `item_revenue_in_usd`, `price_in_usd` | double |
| `total_item_quantity`, `unique_items`, `quantity` | bigint |
| `coupon`, `item_id`, `item_name` | string |

### `silver.sales` (~4k, deduped on `order_id`)

| Column | Type |
|--------|------|
| `order_id`, `email`, `coupon`, `item_id`, `item_name` | string |
| `transaction_timestamp` | timestamp |
| `purchase_revenue_in_usd`, `item_revenue_in_usd`, `price_in_usd` | double |
| `total_item_quantity`, `unique_items`, `quantity` | bigint |

---

## Gold

### `gold.dim_users`

`user_id`, `email`, `first_touch_date`, `user_first_touch_timestamp`

### `gold.dim_products`

`item_id`, `product_name`, `price`

### `gold.fact_sales`

`order_id`, `user_id`, `item_id`, `coupon`, `transaction_timestamp`, `purchase_revenue_in_usd`, `item_revenue_in_usd`, `price_in_usd`, `quantity`, `total_item_quantity`, `unique_items`

### `gold.fact_event`

`user_id`, `device`, `event_name`, `traffic_source`, `event_timestamp`, `event_previous_timestamp`, `user_first_touch_timestamp`, `city`, `state`

---

## DQ (`ecom_clickstream.dq`)

Populated by `_run_one_scan()` — 15 scans per batch (5 pillars × bronze/silver/gold). Pillars: `contracts`, `freshness_and_schema_drift`, `expectations`, `anomalies`, `integrity`.

### `scan_summary`

`scan_id`, `scan_timestamp`, `medallion_layer`, `check_category`, `checks_file`, `checks_total`, `checks_pass`, `checks_warn`, `checks_fail`, `checks_error`, `scan_definition`

### `check_results`

`scan_id`, `scan_timestamp`, `medallion_layer`, `check_category`, `checks_file`, `dataset_name`, `check_name`, `check_type`, `check_definition`, `outcome`, `check_value`, `diagnostics`, `data_source`

### Soda temp views (scan time)

| View | Source |
|------|--------|
| `bronze_*` | Volume JSON, filtered by `entity_type` |
| `silver_*` | `ecom_clickstream.silver.*` |
| `gold_*` | `ecom_clickstream.gold.*` |

Bronze checks use `payload` + `get_json_object()`, not silver columns.

---

## Lineage

```
Raw marketplace volume
  → bronze clickstream_json (JSON Delta)
    → silver.*
      → gold.*
        → dq.* (Soda results)
```

---

*Last updated: 2026-06-07*
