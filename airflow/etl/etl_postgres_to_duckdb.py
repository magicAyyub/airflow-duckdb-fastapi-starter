import psycopg2
import duckdb
import os

PG_HOST = os.getenv("PGHOST", "postgres")
PG_PORT = os.getenv("PGPORT", "5432")
PG_USER = os.getenv("PGUSER", "myuser")
PG_PASSWORD = os.getenv("PGPASSWORD", "mypassword")
PG_DB = os.getenv("PGDATABASE", "metrics")
DUCKDB_FILE = os.getenv("DUCKDB_FILE", "/opt/airflow/olap.duckdb")

def get_last_timestamp():
    conn = duckdb.connect(DUCKDB_FILE)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS metrics (
            id INTEGER,
            timestamp TIMESTAMPTZ,
            value INTEGER
        );
    """)
    result = conn.execute("SELECT MAX(timestamp) FROM metrics;").fetchone()
    conn.close()
    return result[0]  # None if table is empty

def extract_from_postgres(since):
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB
    )
    cur = conn.cursor()
    if since:
        cur.execute("SELECT id, timestamp, value FROM metrics WHERE timestamp > %s ORDER BY timestamp ASC;", (since,))
    else:
        cur.execute("SELECT id, timestamp, value FROM metrics ORDER BY timestamp ASC;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def load_to_duckdb(rows):
    if not rows:
        print("No new rows to insert.")
        return
    conn = duckdb.connect(DUCKDB_FILE)
    conn.executemany("INSERT INTO metrics (id, timestamp, value) VALUES (?, ?, ?);", rows)
    conn.close()

if __name__ == "__main__":
    print("Checking last timestamp in DuckDB...")
    last_ts = get_last_timestamp()
    print(f"Last timestamp: {last_ts}")
    print("Extracting new data from Postgres...")
    data = extract_from_postgres(last_ts)
    print(f"Fetched {len(data)} new rows.")
    print("Loading new data into DuckDB...")
    load_to_duckdb(data)
    print("Done!")