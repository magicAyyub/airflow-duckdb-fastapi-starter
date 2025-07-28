#!/usr/bin/env python3
"""
ETL Script: Synchronisation des données d'opérateurs de PostgreSQL vers DuckDB
"""

import os
import sys
import psycopg2
import duckdb
from datetime import datetime
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration des bases de données
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'metrics'),
    'user': os.getenv('POSTGRES_USER', 'myuser'),
    'password': os.getenv('POSTGRES_PASSWORD', 'mypassword')
}

DUCKDB_PATH = os.getenv('DUCKDB_PATH', '/opt/airflow/olap_data/olap.duckdb')

def get_last_update_timestamp():
    """Récupère le timestamp de la dernière synchronisation depuis DuckDB."""
    try:
        conn = duckdb.connect(DUCKDB_PATH)
        
        # Créer la table de synchronisation si elle n'existe pas
        conn.execute("""
            CREATE TABLE IF NOT EXISTS etl_sync_log (
                table_name VARCHAR,
                last_sync TIMESTAMP,
                records_synced INTEGER,
                PRIMARY KEY (table_name)
            );
        """)
        
        # Récupérer le dernier timestamp
        result = conn.execute("""
            SELECT last_sync FROM etl_sync_log 
            WHERE table_name = 'operators'
        """).fetchone()
        
        conn.close()
        
        if result:
            logger.info(f"Dernière synchronisation: {result[0]}")
            return result[0]
        else:
            logger.info("Première synchronisation - récupération de tous les enregistrements")
            return None
            
    except Exception as e:
        logger.error(f"Erreur lors de la récupération du timestamp: {e}")
        return None

def extract_operators_from_postgres(last_sync=None):
    """Extrait les données d'opérateurs depuis PostgreSQL."""
    try:
        logger.info("Connexion à PostgreSQL...")
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Construire la requête selon la synchronisation incrémentale
        if last_sync:
            query = """
                SELECT * FROM operators 
                WHERE updated_at > %s OR created_at > %s
                ORDER BY id;
            """
            cursor.execute(query, (last_sync, last_sync))
            logger.info(f"Extraction des opérateurs modifiés depuis {last_sync}")
        else:
            query = "SELECT * FROM operators ORDER BY id;"
            cursor.execute(query)
            logger.info("Extraction de tous les opérateurs")
        
        # Récupérer les noms des colonnes
        columns = [desc[0] for desc in cursor.description]
        
        # Récupérer les données
        operators = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        logger.info(f"Extraction terminée: {len(operators)} opérateurs trouvés")
        return operators, columns
        
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction depuis PostgreSQL: {e}")
        return [], []

def load_operators_to_duckdb(operators, columns):
    """Charge les données d'opérateurs télécom dans DuckDB."""
    if not operators:
        logger.info("Aucune donnée à charger")
        return 0
    
    try:
        logger.info("Connexion à DuckDB...")
        conn = duckdb.connect(DUCKDB_PATH)
        
        # Créer la table operators si elle n'existe pas
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS operators (
                id INTEGER PRIMARY KEY,
                first_name VARCHAR,
                birth_name VARCHAR,
                middle_name VARCHAR,
                last_name VARCHAR,
                sex VARCHAR,
                birth_date DATE,
                cogville VARCHAR,
                cogpays VARCHAR,
                birth_city VARCHAR,
                birth_country VARCHAR,
                email VARCHAR,
                created_date DATE,
                archived_date DATE,
                uuid VARCHAR,
                id_ccu VARCHAR,
                subscription_channel VARCHAR,
                verification_mode VARCHAR,
                verification_date DATE,
                user_status VARCHAR,
                two_fa_status VARCHAR,
                first_activation_date DATE,
                expiration_date DATE,
                telephone VARCHAR,
                indicatif VARCHAR,
                date_modif_tel DATE,
                date_modf_tel DATE,
                numero_pi VARCHAR,
                expiration DATE,
                emission DATE,
                type VARCHAR,
                user_uuid VARCHAR,
                identity_verification_mode VARCHAR,
                identity_verification_status VARCHAR,
                identity_verification_result VARCHAR,
                id_identity_verification_proof VARCHAR,
                identity_verification_date DATE,
                operateur VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        
        conn.execute(create_table_sql)
        logger.info("Table operators créée/vérifiée dans DuckDB")
        
        # Construire la requête d'insertion/mise à jour
        placeholders = ','.join(['?' for _ in columns])
        insert_sql = f"""
            INSERT OR REPLACE INTO operators ({','.join(columns)})
            VALUES ({placeholders});
        """
        
        # Insérer les données
        logger.info(f"Insertion de {len(operators)} opérateurs télécom...")
        conn.executemany(insert_sql, operators)
        
        # Mettre à jour le log de synchronisation
        current_time = datetime.now()
        conn.execute("""
            INSERT OR REPLACE INTO etl_sync_log (table_name, last_sync, records_synced)
            VALUES ('operators', ?, ?);
        """, (current_time, len(operators)))
        
        conn.close()
        
        logger.info(f"✅ {len(operators)} opérateurs télécom synchronisés avec succès")
        return len(operators)
        
    except Exception as e:
        logger.error(f"Erreur lors du chargement dans DuckDB: {e}")
        return 0

def create_analytics_views():
    """Crée des vues analytiques télécom dans DuckDB."""
    try:
        conn = duckdb.connect(DUCKDB_PATH)
        
        # Vue des statistiques par opérateur
        conn.execute("""
            CREATE OR REPLACE VIEW operators_by_operateur AS
            SELECT 
                operateur,
                COUNT(*) as total_clients,
                COUNT(CASE WHEN user_status = 'ACTIVE' THEN 1 END) as active_clients,
                COUNT(CASE WHEN two_fa_status = 'ENABLED' THEN 1 END) as clients_2fa,
                COUNT(CASE WHEN identity_verification_status = 'VERIFIED' THEN 1 END) as verified_clients,
                ROUND(COUNT(CASE WHEN user_status = 'ACTIVE' THEN 1 END) * 100.0 / COUNT(*), 2) as active_rate
            FROM operators
            GROUP BY operateur;
        """)
        
        # Vue des performances par canal de souscription
        conn.execute("""
            CREATE OR REPLACE VIEW clients_by_channel AS
            SELECT 
                subscription_channel,
                COUNT(*) as total_clients,
                COUNT(CASE WHEN user_status = 'ACTIVE' THEN 1 END) as active_clients,
                ROUND(AVG(CASE WHEN first_activation_date IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 2) as activation_rate
            FROM operators
            GROUP BY subscription_channel;
        """)
        
        # Vue de l'état de vérification d'identité
        conn.execute("""
            CREATE OR REPLACE VIEW identity_verification_overview AS
            SELECT 
                identity_verification_status,
                identity_verification_mode,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM operators), 2) as percentage
            FROM operators
            GROUP BY identity_verification_status, identity_verification_mode
            ORDER BY count DESC;
        """)
        
        # Vue des clients par statut et 2FA
        conn.execute("""
            CREATE OR REPLACE VIEW security_overview AS
            SELECT 
                user_status,
                two_fa_status,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM operators), 2) as percentage
            FROM operators
            GROUP BY user_status, two_fa_status
            ORDER BY count DESC;
        """)
        
        conn.close()
        logger.info("Vues analytiques télécom créées avec succès")
        
    except Exception as e:
        logger.error(f"Erreur lors de la création des vues: {e}")

def run_etl():
    """Exécute le processus ETL complet."""
    logger.info("🔄 Démarrage de l'ETL operators PostgreSQL -> DuckDB")
    
    try:
        # 1. Récupérer le timestamp de la dernière synchronisation
        last_sync = get_last_update_timestamp()
        
        # 2. Extraire les données depuis PostgreSQL
        operators, columns = extract_operators_from_postgres(last_sync)
        
        # 3. Charger les données dans DuckDB
        records_synced = load_operators_to_duckdb(operators, columns)
        
        # 4. Créer les vues analytiques
        create_analytics_views()
        
        logger.info(f"✅ ETL terminé avec succès - {records_synced} enregistrements synchronisés")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur ETL: {e}")
        return False

if __name__ == "__main__":
    success = run_etl()
    sys.exit(0 if success else 1)
