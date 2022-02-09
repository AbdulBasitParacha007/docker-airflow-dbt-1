#!/bin/bash

set -euo pipefail

python3 -m venv dbt-env
source dbt-env/bin/activate

PIP_USER=false && pip install --no-cache-dir -r $1

# Deactivate the virtual env
deactivate

