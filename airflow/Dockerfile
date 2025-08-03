FROM apache/airflow:2.8.0-python3.11

COPY etl /opt/etl

USER airflow
RUN pip install --user --no-cache-dir \
    duckdb \
    psycopg2-binary \
    pandas \
    faker \
    python-dateutil