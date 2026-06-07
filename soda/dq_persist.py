"""Build Soda result rows and save to ecom_clickstream.dq monitoring tables."""

import json
from collections import Counter

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dq_data import DATA_SOURCE, checks_relpath, scan_definition

DQ_SCHEMA = "ecom_clickstream.dq"
SUMMARY_TABLE = "ecom_clickstream.dq.scan_summary"
RESULTS_TABLE = "ecom_clickstream.dq.check_results"

SUMMARY_SCHEMA = StructType(
    [
        StructField("scan_id", StringType()),
        StructField("scan_timestamp", TimestampType()),
        StructField("medallion_layer", StringType()),
        StructField("check_category", StringType()),
        StructField("checks_file", StringType()),
        StructField("checks_total", LongType()),
        StructField("checks_pass", LongType()),
        StructField("checks_warn", LongType()),
        StructField("checks_fail", LongType()),
        StructField("checks_error", LongType()),
        StructField("scan_definition", StringType()),
    ]
)

RESULTS_SCHEMA = StructType(
    [
        StructField("scan_id", StringType()),
        StructField("scan_timestamp", TimestampType()),
        StructField("medallion_layer", StringType()),
        StructField("check_category", StringType()),
        StructField("checks_file", StringType()),
        StructField("dataset_name", StringType()),
        StructField("check_name", StringType()),
        StructField("check_type", StringType()),
        StructField("check_definition", StringType()),
        StructField("outcome", StringType()),
        StructField("check_value", DoubleType()),
        StructField("diagnostics", StringType()),
        StructField("data_source", StringType()),
    ]
)


def build_rows(scan, scan_id, scan_time, layer, pillar):
    """Turn finished Soda checks into rows for the check_results table."""
    # dq_data.checks_relpath — path stored in dq tables for this layer/pillar YAML
    checks_file = checks_relpath(layer, pillar)
    rows = []
    for check in scan._checks:
        if check.outcome is None:
            continue

        info = check.get_dict()
        raw_value = check.check_value
        value = float(raw_value) if isinstance(raw_value, (int, float)) else None

        rows.append(
            {
                "scan_id": scan_id,
                "scan_timestamp": scan_time,
                "medallion_layer": layer,
                "check_category": pillar,
                "checks_file": checks_file,
                "dataset_name": str(info.get("table") or ""),
                "check_name": str(check.name or ""),
                "check_type": str(check.cloud_check_type or ""),
                "check_definition": str(check.create_definition()),
                "outcome": str(check.outcome.value),
                "check_value": value,
                "diagnostics": json.dumps(info.get("diagnostics", {}), default=str),
                "data_source": DATA_SOURCE,
            }
        )

    return rows


def create_dq_tables(spark):
    """Create monitoring tables. Returns a log message."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DQ_SCHEMA}")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SUMMARY_TABLE} (
          scan_id STRING,
          scan_timestamp TIMESTAMP,
          medallion_layer STRING,
          check_category STRING,
          checks_file STRING,
          checks_total LONG,
          checks_pass LONG,
          checks_warn LONG,
          checks_fail LONG,
          checks_error LONG,
          scan_definition STRING
        )
        USING DELTA
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
          scan_id STRING,
          scan_timestamp TIMESTAMP,
          medallion_layer STRING,
          check_category STRING,
          checks_file STRING,
          dataset_name STRING,
          check_name STRING,
          check_type STRING,
          check_definition STRING,
          outcome STRING,
          check_value DOUBLE,
          diagnostics STRING,
          data_source STRING
        )
        USING DELTA
        """
    )

    return f"Tables ready: {SUMMARY_TABLE}, {RESULTS_TABLE}"


def summarize_checks(rows):
    """Aggregate outcome counts from check result rows."""
    counts = Counter(row["outcome"] for row in rows)
    return {
        "checks_total": len(rows),
        "checks_pass": counts.get("pass", 0),
        "checks_warn": counts.get("warn", 0),
        "checks_fail": counts.get("fail", 0),
        "checks_error": counts.get("error", 0),
    }


def save_scan(spark, scan_id, layer, pillar, rows, scan_timestamp):
    """Append one scan to scan_summary and check_results."""
    # dq_data — relative YAML path and Soda scan name for this layer/pillar
    checks_file = checks_relpath(layer, pillar)
    counts = Counter(row["outcome"] for row in rows)
    summary = {
        "scan_id": scan_id,
        "scan_timestamp": scan_timestamp,
        "medallion_layer": layer,
        "check_category": pillar,
        "checks_file": checks_file,
        "checks_total": len(rows),
        "checks_pass": counts.get("pass", 0),
        "checks_warn": counts.get("warn", 0),
        "checks_fail": counts.get("fail", 0),
        "checks_error": counts.get("error", 0),
        "scan_definition": scan_definition(layer, pillar),
    }

    spark.createDataFrame([summary], schema=SUMMARY_SCHEMA).write.format("delta").mode("append").saveAsTable(
        SUMMARY_TABLE
    )

    if rows:
        spark.createDataFrame(rows, schema=RESULTS_SCHEMA).write.format("delta").mode("append").saveAsTable(
            RESULTS_TABLE
        )
