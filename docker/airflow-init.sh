#!/usr/bin/env bash
set -euo pipefail

airflow db migrate
# Re-running compose up must not fail because the admin already exists.
python - <<'PY'
import json
import os
import subprocess

users = json.loads(subprocess.check_output(["airflow", "users", "list", "--output", "json"]))
username = os.environ["AIRFLOW_ADMIN_USERNAME"]
if not any(user["username"] == username for user in users):
    subprocess.run(
        ["airflow", "users", "create", "--username", username,
         "--password", os.environ["AIRFLOW_ADMIN_PASSWORD"],
         "--firstname", "Pipeline", "--lastname", "Admin", "--role", "Admin",
         "--email", os.environ["AIRFLOW_ADMIN_EMAIL"]],
        check=True,
    )
PY
alembic upgrade head
