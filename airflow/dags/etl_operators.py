#!/usr/bin/env python3
"""
DAG Airflow: Synchronisation des données d'opérateurs PostgreSQL -> DuckDB
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

# Ajouter le répertoire ETL au path
sys.path.append('/opt/airflow/etl')

# Import du script ETL
try:
    from etl_operators import run_etl
except ImportError:
    def run_etl():
        """Fallback function si l'import échoue"""
        import subprocess
        result = subprocess.run([
            'python', '/opt/airflow/etl/etl_operators.py'
        ], capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"ETL failed: {result.stderr}")
        return result.stdout

# Configuration par défaut du DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

# Définition du DAG
dag = DAG(
    'etl_operators',
    default_args=default_args,
    description='Synchronisation des données d\'opérateurs PostgreSQL vers DuckDB',
    schedule_interval=timedelta(hours=1),  # Exécution toutes les heures
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'operators', 'postgres', 'duckdb']
)

def check_postgres_connection():
    """Vérifie la connexion à PostgreSQL."""
    import psycopg2
    
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=os.getenv('POSTGRES_PORT', '5432'),
            database=os.getenv('POSTGRES_DB', 'metrics'),
            user=os.getenv('POSTGRES_USER', 'myuser'),
            password=os.getenv('POSTGRES_PASSWORD', 'mypassword')
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM operators;")
        count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f"✅ PostgreSQL accessible - {count} opérateurs trouvés")
        return True
        
    except Exception as e:
        print(f"❌ Erreur de connexion PostgreSQL: {e}")
        raise

def check_duckdb_access():
    """Vérifie l'accès à DuckDB."""
    import duckdb
    
    try:
        duckdb_path = '/opt/airflow/olap_data/olap.duckdb'
        
        # Créer le répertoire si nécessaire
        os.makedirs(os.path.dirname(duckdb_path), exist_ok=True)
        
        conn = duckdb.connect(duckdb_path)
        
        # Test basique
        conn.execute("SELECT 1 as test;")
        result = conn.fetchone()
        
        conn.close()
        
        print(f"✅ DuckDB accessible - Test result: {result[0]}")
        return True
        
    except Exception as e:
        print(f"❌ Erreur d'accès DuckDB: {e}")
        raise

def execute_etl_process():
    """Exécute le processus ETL."""
    try:
        print("🔄 Démarrage du processus ETL...")
        result = run_etl()
        print("✅ ETL terminé avec succès")
        return result
    except Exception as e:
        print(f"❌ Erreur ETL: {e}")
        raise

def generate_etl_report():
    """Génère un rapport sur l'état de la synchronisation."""
    import duckdb
    
    try:
        conn = duckdb.connect('/opt/airflow/olap_data/olap.duckdb')
        
        # Statistiques générales
        conn.execute("SELECT COUNT(*) FROM operators;")
        total_operators = conn.fetchone()[0]
        
        # Dernière synchronisation
        conn.execute("""
            SELECT last_sync, records_synced 
            FROM etl_sync_log 
            WHERE table_name = 'operators'
        """)
        sync_info = conn.fetchone()
        
        # Répartition par service
        conn.execute("""
            SELECT service, COUNT(*) as count
            FROM operators 
            GROUP BY service 
            ORDER BY count DESC;
        """)
        service_stats = conn.fetchall()
        
        conn.close()
        
        # Générer le rapport
        report = f"""
📊 RAPPORT ETL OPERATORS - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
═══════════════════════════════════════════════════════════════

📈 STATISTIQUES GÉNÉRALES:
  • Total opérateurs: {total_operators}
  • Dernière sync: {sync_info[0] if sync_info else 'N/A'}
  • Enregistrements synchronisés: {sync_info[1] if sync_info else 'N/A'}

📊 RÉPARTITION PAR SERVICE:
"""
        for service, count in service_stats:
            report += f"  • {service}: {count} opérateurs\n"
        
        print(report)
        return report
        
    except Exception as e:
        print(f"❌ Erreur génération rapport: {e}")
        raise

# Définition des tâches
task_check_postgres = PythonOperator(
    task_id='check_postgres_connection',
    python_callable=check_postgres_connection,
    dag=dag
)

task_check_duckdb = PythonOperator(
    task_id='check_duckdb_access',
    python_callable=check_duckdb_access,
    dag=dag
)

task_etl = PythonOperator(
    task_id='execute_etl_process',
    python_callable=execute_etl_process,
    dag=dag
)

task_report = PythonOperator(
    task_id='generate_etl_report',
    python_callable=generate_etl_report,
    dag=dag
)

# Définition des dépendances
[task_check_postgres, task_check_duckdb] >> task_etl >> task_report
