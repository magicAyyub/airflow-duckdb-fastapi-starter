import os
import psycopg2
import time
from random import randint
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    dbname=os.getenv("PGDATABASE"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD"),
    host=os.getenv("PGHOST"),
    port=os.getenv("PGPORT")
)

cursor = conn.cursor()

print("🌱 Démarrage de l'injection des données...")

try:
    while True:
        value = randint(0, 100)
        cursor.execute("INSERT INTO metrics (value) VALUES (%s)", (value,))
        conn.commit()
        print(f"✅ Nouvelle entrée : {value}")
        time.sleep(1)
except KeyboardInterrupt:
    print("🛑 Arrêt manuel.")
finally:
    cursor.close()
    conn.close()