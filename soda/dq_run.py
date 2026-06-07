"""Run Soda checks and write results to Delta tables."""

import uuid
from datetime import datetime, timezone

from soda.scan import Scan

from dq_data import (
    DATA_SOURCE,
    FRAMEWORK_PILLARS,
    MEDALLION_LAYERS,
    get_checks_file,
    register_views,
    scan_definition,
)
from dq_persist import build_rows, create_dq_tables, save_scan, summarize_checks


def _run_one_scan(spark, soda_dir, registry, layers=None):
    """
    Run Soda scans for all pillars in the given layers.

    layers: list like ["bronze", "silver", "gold"] — default runs all three.
    Prints each check, saves to ecom_clickstream.dq tables, returns scan_id.
    """
    layers = layers or MEDALLION_LAYERS
    scan_id = str(uuid.uuid4())
    scan_time = datetime.now(timezone.utc)

    # dq_persist.create_dq_tables — ensure ecom_clickstream.dq schema/tables exist
    print(create_dq_tables(spark))

    all_rows = []

    for layer in layers:
        for pillar in FRAMEWORK_PILLARS:
            print(f"\n--- {layer} / {pillar} ---")

            # dq_data.get_checks_file — absolute path to the Soda YAML for this layer/pillar
            yaml_path = get_checks_file(soda_dir, layer, pillar)

            # dq_data.register_views — create temp views from datasets.yml before Soda runs
            register_views(spark, registry, layer, pillar)

            scan = Scan()
            scan.set_scan_definition_name(scan_definition(layer, pillar))
            scan.set_data_source_name(DATA_SOURCE)
            scan.add_spark_session(spark, data_source_name=DATA_SOURCE)
            scan.add_sodacl_yaml_file(yaml_path)
            scan.execute()

            print(scan.get_all_checks_text())

            # dq_persist.build_rows — convert Soda check outcomes to check_results row dicts
            rows = build_rows(scan, scan_id, scan_time, layer, pillar)

            # dq_persist.save_scan — append summary + check rows to Delta dq tables
            save_scan(spark, scan_id, layer, pillar, rows, scan_time)
            all_rows.extend(rows)

    # dq_persist.summarize_checks — count pass/warn/fail/error across the batch
    stats = summarize_checks(all_rows)
    passed = stats["checks_fail"] == 0 and stats["checks_error"] == 0
    print(f"\n=== BATCH {scan_id} ===")
    print(
        f"pass={stats['checks_pass']} warn={stats['checks_warn']} "
        f"fail={stats['checks_fail']} error={stats['checks_error']} "
        f"total={stats['checks_total']}"
    )
    print("PASSED" if passed else "FAILED")
    return scan_id
