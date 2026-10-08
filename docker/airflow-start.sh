#!/usr/bin/env bash
set -euo pipefail

airflow scheduler &
scheduler_pid=$!
airflow webserver &
webserver_pid=$!

cleanup() {
    kill -TERM "$scheduler_pid" "$webserver_pid" 2>/dev/null || true
    wait "$scheduler_pid" "$webserver_pid" 2>/dev/null || true
}
trap cleanup EXIT
trap 'exit 0' TERM INT
# Restart the container if either process exits.
wait -n "$scheduler_pid" "$webserver_pid"
exit 1
