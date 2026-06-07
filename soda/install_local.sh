#!/usr/bin/env bash
# Install Soda Core (no Soda Cloud required) alongside Databricks Connect.
# Usage: ./soda/install_local.sh

set -euo pipefail
cd "$(dirname "$0")/.."
source .venv/bin/activate

SODA_VERSION="3.3.22"

# --no-deps avoids soda pulling its own pyspark (conflicts with databricks-connect)
pip install "soda-core==${SODA_VERSION}" \
            "soda-core-spark==${SODA_VERSION}" \
            "soda-core-spark-df==${SODA_VERSION}" \
            --no-deps

# Restore pyspark 4.x bundled with databricks-connect
pip uninstall -y pyspark 2>/dev/null || true
pip install --force-reinstall --no-deps databricks-connect
pip install "protobuf>=6.33.5,<7"

python -c "
from soda.scan import Scan
from databricks.connect import DatabricksSession
print('Soda Core', Scan)
print('Databricks Connect OK')
"
