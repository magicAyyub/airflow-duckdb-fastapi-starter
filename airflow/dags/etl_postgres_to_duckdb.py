from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add the ETL directory to Python path
sys.path.append('/opt/airflow/etl')

def check_postgres_connection():
    """Vérifie la connexion à PostgreSQL."""
    import psycopg2
    try:
        conn = psycopg2.connect(
            host=os.getenv("PGHOST", "postgres"),
            port=os.getenv("PGPORT", "5432"),
            user=os.getenv("PGUSER", "myuser"),
            password=os.getenv("PGPASSWORD", "mypassword"),
            dbname=os.getenv("PGDATABASE", "metrics")
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM user_data;")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        print(f"=== PostgreSQL OK - {count} utilisateurs dans la base")
        return count
    except Exception as e:
        print(f"=== Erreur PostgreSQL: {e}")
        raise

def check_duckdb_status():
    """Vérifie le statut de DuckDB."""
    import duckdb
    import time
    
    # Essayer d'abord en lecture seule si le fichier est verrouillé
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Essayer une connexion normale d'abord
            conn = duckdb.connect('/opt/airflow/olap.duckdb')
            break
        except duckdb.IOException as e:
            if "Conflicting lock" in str(e) and attempt < max_retries - 1:
                print(f"=== DuckDB verrouillé, tentative {attempt + 1}/{max_retries}, attente 5s...")
                time.sleep(5)
                continue
            elif "Conflicting lock" in str(e):
                # Utiliser le mode lecture seule en dernier recours
                print("=== Utilisation du mode lecture seule pour DuckDB")
                conn = duckdb.connect('/opt/airflow/olap.duckdb', read_only=True)
                break
            else:
                raise e
    
    try:
        # Vérifier si la table existe
        result = conn.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name = 'user_data_with_operators'
        """).fetchone()
        
        if result[0] > 0:
            count = conn.execute("SELECT COUNT(*) FROM user_data_with_operators;").fetchone()[0]
            last_sync = conn.execute("""
                SELECT last_sync, records_synced FROM etl_sync_log 
                WHERE table_name = 'user_data_with_operators'
            """).fetchone()
            
            conn.close()
            
            if last_sync:
                print(f"=== DuckDB OK - {count} enregistrements, dernière sync: {last_sync[0]} ({last_sync[1]} records)")
            else:
                print(f"=== DuckDB OK - {count} enregistrements, pas de sync précédente")
        else:
            conn.close()
            print("ℹ️  Table user_data_with_operators n'existe pas encore dans DuckDB")
        
    except Exception as e:
        print(f"=== Erreur DuckDB: {e}")
        raise

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='user_data_etl_with_operators',
    default_args=default_args,
    description='Extract user data from PostgreSQL, join with operator mapping, and load into DuckDB for OLAP queries',
    schedule_interval='@hourly',  # Runs every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'user-data', 'operators', 'postgres', 'duckdb', 'olap']
) as dag:

    # Task 1: Check PostgreSQL connection and data
    check_postgres = PythonOperator(
        task_id='check_postgres_connection',
        python_callable=check_postgres_connection,
        doc_md="""
        ## Check PostgreSQL Connection
        
        Vérifie que la connexion à PostgreSQL fonctionne et compte les utilisateurs.
        """
    )

    # Task 2: Check DuckDB status
    check_duckdb = PythonOperator(
        task_id='check_duckdb_status',
        python_callable=check_duckdb_status,
        doc_md="""
        ## Check DuckDB Status
        
        Vérifie l'état de la base DuckDB et affiche les informations de synchronisation.
        """
    )

    # Task 3: Run the complete ETL pipeline
    run_etl_pipeline = BashOperator(
        task_id='run_user_data_etl',
        bash_command='python /opt/airflow/etl/etl_postgres_to_duckdb.py',
        doc_md="""
        ## Run User Data ETL Pipeline
        
        Exécute le pipeline ETL complet:
        1. Extraction des données utilisateur depuis PostgreSQL
        2. Extraction du mapping des opérateurs
        3. Jointure des données utilisateur avec les opérateurs télécom
        4. Chargement dans DuckDB pour les requêtes OLAP
        
        Le pipeline traite uniquement les données modifiées depuis la dernière synchronisation.
        """
    )

    # Define task dependencies
    [check_postgres, check_duckdb] >> run_etl_pipeline

# Documentation for the DAG
dag.doc_md = """
# User Data ETL with Operator Mapping

Ce DAG automatise l'extraction, la transformation et le chargement des données utilisateur 
avec enrichissement des informations d'opérateur télécom.

## Processus

1. **Vérifications préliminaires**: Connexion PostgreSQL et statut DuckDB
2. **Extraction**: Récupération des données utilisateur et mapping opérateurs depuis PostgreSQL
3. **Transformation**: Jointure des données basée sur les préfixes téléphoniques français
4. **Chargement**: Insertion dans DuckDB pour les requêtes analytiques

## Fréquence

- **Planification**: Toutes les heures
- **Mode**: Synchronisation incrémentale (seulement les nouvelles/modifiées)
- **Rétentions**: 2 tentatives avec délai de 5 minutes

## Tables

- **Source**: `user_data` et `operator_mapping` (PostgreSQL)
- **Destination**: `user_data_with_operators` (DuckDB)
- **Log**: `etl_sync_log` (DuckDB)
"""