-- Read credentials from the container environment, then quote as SQL literals.
\getenv pipeline_password PIPELINE_DB_PASSWORD
\getenv airflow_password AIRFLOW_DB_PASSWORD
CREATE USER job_pipeline WITH PASSWORD :'pipeline_password';
CREATE DATABASE job_pipeline OWNER job_pipeline;
CREATE USER airflow WITH PASSWORD :'airflow_password';
CREATE DATABASE airflow OWNER airflow;
REVOKE ALL ON DATABASE job_pipeline FROM PUBLIC;
REVOKE ALL ON DATABASE airflow FROM PUBLIC;
