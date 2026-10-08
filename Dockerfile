FROM apache/airflow:2.10.5-python3.12

USER airflow
WORKDIR /opt/airflow

COPY --chown=airflow:root pyproject.toml README.md ./
COPY --chown=airflow:root src/ ./src/
# The base image ships the matching Airflow/Python constraints. pip keeps
# satisfying installed packages; only missing pipeline dependencies are added.
RUN pip install --no-cache-dir --timeout 120 --retries 3 \
        --constraint /home/airflow/constraints.txt . \
    && pip check

COPY --chown=airflow:root dags/ ./dags/
COPY --chown=airflow:root migrations/ ./migrations/
COPY --chown=airflow:root alembic.ini ./
COPY --chown=airflow:root docker/airflow-init.sh docker/airflow-start.sh ./docker/
