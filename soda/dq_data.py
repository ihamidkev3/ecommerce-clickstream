"""Paths, dataset registry, and Spark temp views for Soda scans."""

import os

import yaml
from pyspark.sql.functions import col

FRAMEWORK_PILLARS = [
    "contracts",
    "freshness_and_schema_drift",
    "expectations",
    "anomalies",
    "integrity",
]

MEDALLION_LAYERS = ["bronze", "silver", "gold"]
DATA_SOURCE = "ecom_clickstream"

_bronze_views_registered = False
_layer_views_registered = set()


def find_soda_dir():
    return os.path.dirname(os.path.abspath(__file__))


def checks_relpath(layer, pillar):
    return f"checks/{layer}/{pillar}.yml"


def scan_definition(layer, pillar):
    return f"{DATA_SOURCE}_{layer}_{pillar}"


def load_datasets(soda_dir):
    config_path = os.path.join(soda_dir, "config", "datasets.yml")
    with open(config_path) as file:
        return yaml.safe_load(file)


def get_checks_file(soda_dir, layer, pillar):
    return os.path.join(soda_dir, checks_relpath(layer, pillar))


def _register_bronze_views(spark, registry):
    global _bronze_views_registered
    if _bronze_views_registered:
        return

    bronze_entries = registry["bronze"]
    source_key = next(iter(bronze_entries.values()))["source"]
    bronze_df = spark.read.format("delta").load(registry["sources"][source_key]["path"])

    for view_name, info in bronze_entries.items():
        entity = info.get("entity")
        if entity:
            bronze_df.filter(col("entity_type") == entity).createOrReplaceTempView(view_name)
        else:
            bronze_df.createOrReplaceTempView(view_name)

    _bronze_views_registered = True


def _register_layer_views(spark, registry, layer):
    global _layer_views_registered
    if layer in _layer_views_registered:
        return

    for view_name, info in registry[layer].items():
        spark.read.table(info["name"]).createOrReplaceTempView(view_name)
    _layer_views_registered.add(layer)


def register_views(spark, registry, layer, pillar):
    """Register temp views for one scan. Bronze also loads for silver integrity checks."""
    need_bronze = layer == "bronze" or (layer == "silver" and pillar == "integrity")
    if need_bronze:
        _register_bronze_views(spark, registry)
    if layer in ("silver", "gold"):
        _register_layer_views(spark, registry, layer)
