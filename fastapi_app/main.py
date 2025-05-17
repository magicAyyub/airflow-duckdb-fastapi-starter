from fastapi import FastAPI
import duckdb
from fastapi.responses import StreamingResponse
import time
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Ajoute ce bloc juste après la création de l'app
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Pour la démo, autorise tout. En prod, restreins !
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DUCKDB_FILE = "/opt/airflow/olap.duckdb"  # Chemin du volume partagé

@app.get("/metrics")
def get_metrics(limit: int = 100):
    try:
        con = duckdb.connect(DUCKDB_FILE, read_only=True)
        rows = con.execute("SELECT * FROM metrics ORDER BY timestamp DESC LIMIT ?", [limit]).fetchall()
        con.close()
        return [
            {"id": r[0], "timestamp": str(r[1]), "value": r[2]}
            for r in rows
        ]
    except Exception as e:
        return {"error": str(e)}

@app.get("/metrics/avg")
def get_avg():
    con = duckdb.connect(DUCKDB_FILE, read_only=True)
    avg = con.execute("SELECT AVG(value) FROM metrics").fetchone()[0]
    con.close()
    return {"average": avg}

@app.get("/stream")
def stream():
    def event_stream():
        for i in range(10):
            yield f"data: {i}\n\n"
            time.sleep(1)
    return StreamingResponse(event_stream(), media_type="text/event-stream")