"""
Bronze JSON helpers for the Bronze ingest notebook.

Bronze stores one JSON object per row:
  entity_type, payload, _ingested_at, _source_file
"""

from pyspark.sql.functions import col, lit, struct, to_json

# Soda scans: same path in soda/config/datasets.yml → sources.bronze_json.path
BRONZE_JSON_PATH = "/Volumes/ecom_clickstream/bronze/delta/clickstream_json/"
ENTITIES = ["users", "products", "events", "sales", "item_lookup"]


def to_bronze_json_rows(df, entity_type):
    """Wrap each source row as one JSON payload."""
    if entity_type == "sales" and "items" in df.columns:
        if dict(df.dtypes)["items"].startswith("array"):
            df = df.withColumn("items", to_json(col("items")))

    data_cols = [c for c in df.columns if c not in ("_ingested_at", "_source_file")]
    return df.select(
        lit(entity_type).alias("entity_type"),
        to_json(struct(*[col(c) for c in data_cols])).alias("payload"),
        col("_ingested_at"),
        col("_source_file"),
    )


def load_bronze(spark):
    """Read the bronze JSON Delta table."""
    return spark.read.format("delta").load(BRONZE_JSON_PATH)
