#!/usr/bin/env python3
"""
ETL Script: Complete User Data and Operator Mapping Pipeline
Extracts user data from PostgreSQL, joins with operator mapping, and loads into DuckDB
"""

import os
import psycopg2
import duckdb
import pandas as pd
from datetime import datetime, timezone
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PG_HOST = os.getenv("PGHOST", "postgres")
PG_PORT = os.getenv("PGPORT", "5432")
PG_USER = os.getenv("PGUSER", "myuser")
PG_PASSWORD = os.getenv("PGPASSWORD", "mypassword")
PG_DB = os.getenv("PGDATABASE", "metrics")
DUCKDB_FILE = os.getenv("DUCKDB_FILE", "/opt/airflow/olap.duckdb")

def get_last_sync_timestamp():
    """Récupère le timestamp de la dernière synchronisation depuis DuckDB."""
    import time
    
    max_retries = 5
    for attempt in range(max_retries):
        try:
            conn = duckdb.connect(DUCKDB_FILE)
            
            # Créer la table de synchronisation si elle n'existe pas
            conn.execute("""
                CREATE TABLE IF NOT EXISTS etl_sync_log (
                    table_name VARCHAR,
                    last_sync TIMESTAMPTZ,
                    records_synced INTEGER,
                    etl_duration_seconds INTEGER,
                    PRIMARY KEY (table_name)
                );
            """)
            
            # Récupérer le dernier timestamp pour user_data
            result = conn.execute("""
                SELECT last_sync FROM etl_sync_log 
                WHERE table_name = 'user_data_with_operators'
            """).fetchone()
            
            conn.close()
            
            if result:
                logger.info(f"Dernière synchronisation: {result[0]}")
                return result[0]
            else:
                logger.info("Première synchronisation - récupération de tous les enregistrements")
                return None
                
        except duckdb.IOException as e:
            if "Conflicting lock" in str(e) and attempt < max_retries - 1:
                logger.warning(f"DuckDB verrouillé, tentative {attempt + 1}/{max_retries}, attente {2 ** attempt}s...")
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
            else:
                logger.error(f"Erreur lors de la récupération du timestamp: {e}")
                return None
        except Exception as e:
            logger.error(f"Erreur lors de la récupération du timestamp: {e}")
            return None
    
    return None

def extract_user_data_from_postgres(last_sync=None):
    """Extrait les données utilisateur depuis PostgreSQL."""
    try:
        logger.info("Connexion à PostgreSQL...")
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DB
        )
        
        # Extract user data with explicit column selection to ensure all columns are included
        if last_sync:
            user_query = """
                SELECT id, first_name, birth_name, middle_name, last_name, sex, birth_date, 
                       cogville, cogpays, birth_city, birth_country, email, created_date, 
                       uuid, id_ccu, subscription_channel, verification_mode, verification_date, 
                       user_status, tfa_status, first_activation_date, expiration_date, 
                       telephone, indicatif, date_modif_tel, numero_pi, expiration_doc, 
                       emission_doc, type_doc, user_uuid, identity_verification_mode, 
                       identity_verification_status, identity_verification_result, 
                       id_identity_verification_proof, identity_verification_date, updated_at
                FROM user_data 
                WHERE updated_at > %s OR created_date > %s
                ORDER BY id;
            """
            user_df = pd.read_sql_query(user_query, conn, params=[last_sync, last_sync])
            logger.info(f"Extraction des utilisateurs modifiés depuis {last_sync}")
        else:
            user_query = """
                SELECT id, first_name, birth_name, middle_name, last_name, sex, birth_date, 
                       cogville, cogpays, birth_city, birth_country, email, created_date, 
                       uuid, id_ccu, subscription_channel, verification_mode, verification_date, 
                       user_status, tfa_status, first_activation_date, expiration_date, 
                       telephone, indicatif, date_modif_tel, numero_pi, expiration_doc, 
                       emission_doc, type_doc, user_uuid, identity_verification_mode, 
                       identity_verification_status, identity_verification_result, 
                       id_identity_verification_proof, identity_verification_date, updated_at
                FROM user_data 
                ORDER BY id;
            """
            user_df = pd.read_sql_query(user_query, conn)
            logger.info("Extraction de tous les utilisateurs")
        
        # Extract operator mapping
        operator_query = "SELECT * FROM operator_mapping ORDER BY id;"
        operator_df = pd.read_sql_query(operator_query, conn)
        
        conn.close()
        
        logger.info(f"Extraction terminée: {len(user_df)} utilisateurs ({user_df.shape[1]} colonnes), {len(operator_df)} mappings d'opérateurs")
        logger.info(f"Colonnes utilisateur: {list(user_df.columns)}")
        return user_df, operator_df
        
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction depuis PostgreSQL: {e}")
        return pd.DataFrame(), pd.DataFrame()

def join_operator_data(user_df, operator_df):
    """
    Joint les données utilisateur avec les informations d'opérateur basées sur les préfixes téléphoniques.
    Utilise la vraie table MAJNUM avec des préfixes granulaires.
    """
    if user_df.empty or operator_df.empty:
        logger.warning("DataFrames vides, impossible de faire la jointure")
        return user_df
    
    try:
        logger.info("Début de la jointure avec les données d'opérateur MAJNUM...")
        
        # Nettoyer les numéros de téléphone
        user_df['telephone'] = user_df['telephone'].astype(str)
        user_df['telephone'] = user_df['telephone'].str.replace('+', '')
        user_df['telephone'] = user_df['telephone'].str.replace('.0', '')
        
        # Préparer les données d'opérateur - trier par longueur de préfixe (desc) pour matcher les plus spécifiques d'abord
        operator_mapping = operator_df.copy()
        operator_mapping = operator_mapping.rename(columns={
            'ezabpqm': 'prefixe',
            'mnemo': 'operateur'
        })
        
        # Ajouter une colonne pour la longueur du préfixe pour prioriser les matches
        operator_mapping['prefix_length'] = operator_mapping['prefixe'].astype(str).str.len()
        operator_mapping = operator_mapping.sort_values('prefix_length', ascending=False)
        
        # Séparer les numéros français et étrangers
        user_df_fr = user_df[user_df['telephone'].str[:2] == '33'].copy()
        user_df_non_fr = user_df[user_df['telephone'].str[:2] != '33'].copy()
        
        if not user_df_fr.empty:
            # Retirer le code pays (33) pour les numéros français
            user_df_fr['telephone_clean'] = user_df_fr['telephone'].str[2:]
            
            # Initialiser la colonne operateur
            user_df_fr['operateur'] = None
            
            # Pour chaque enregistrement d'opérateur, du plus spécifique au plus général
            for _, op_row in operator_mapping.iterrows():
                prefix = str(op_row['prefixe'])
                operateur = op_row['operateur']
                
                # Trouver les numéros qui matchent ce préfixe et qui n'ont pas encore d'opérateur assigné
                mask = (user_df_fr['telephone_clean'].str.startswith(prefix)) & (user_df_fr['operateur'].isna())
                
                if mask.any():
                    user_df_fr.loc[mask, 'operateur'] = operateur
                    matched_count = mask.sum()
                    logger.debug(f"Préfixe {prefix} -> {operateur}: {matched_count} numéros matchés")
            
            # Pour les numéros français non matchés, assigner "INCONNU"
            unmatched_mask = user_df_fr['operateur'].isna()
            if unmatched_mask.any():
                user_df_fr.loc[unmatched_mask, 'operateur'] = 'INCONNU'
                logger.info(f"{unmatched_mask.sum()} numéros français non matchés marqués comme INCONNU")
            
            # Nettoyer les colonnes temporaires
            user_df_fr = user_df_fr.drop(['telephone_clean'], axis=1)
            
            logger.info(f"Jointure réussie pour {len(user_df_fr)} numéros français")
        
        # Ajouter une colonne operateur pour les numéros non-français
        if not user_df_non_fr.empty:
            user_df_non_fr['operateur'] = 'ETRANGER'
            logger.info(f"{len(user_df_non_fr)} numéros étrangers marqués")
        
        # Combiner les résultats
        if not user_df_fr.empty and not user_df_non_fr.empty:
            result_df = pd.concat([user_df_fr, user_df_non_fr], ignore_index=True)
        elif not user_df_fr.empty:
            result_df = user_df_fr
        elif not user_df_non_fr.empty:
            result_df = user_df_non_fr
        else:
            result_df = user_df.copy()
            result_df['operateur'] = None
        
        # Statistiques de mapping
        if 'operateur' in result_df.columns:
            op_stats = result_df['operateur'].value_counts()
            logger.info(f"Répartition des opérateurs:")
            for op, count in op_stats.items():
                logger.info(f"  {op}: {count} utilisateurs")
        
        logger.info(f"Jointure terminée avec succès: {len(result_df)} enregistrements finaux")
        return result_df
        
    except Exception as e:
        logger.error(f"Erreur lors de la jointure des données d'opérateur: {e}")
        # En cas d'erreur, retourner les données originales avec une colonne operateur vide
        user_df['operateur'] = None
        return user_df

def load_to_duckdb(df, etl_start_time):
    """Charge les données enrichies dans DuckDB."""
    if df.empty:
        logger.info("Aucune nouvelle donnée à charger.")
        return 0
    
    import time
    max_retries = 5
    
    for attempt in range(max_retries):
        try:
            conn = duckdb.connect(DUCKDB_FILE)
            
            # Créer la table principale si elle n'existe pas
            conn.execute("""
                CREATE TABLE IF NOT EXISTS user_data_with_operators (
                    id INTEGER,
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
                    created_date TIMESTAMPTZ,
                    uuid VARCHAR,
                    id_ccu VARCHAR,
                    subscription_channel VARCHAR,
                    verification_mode VARCHAR,
                    verification_date TIMESTAMPTZ,
                    user_status VARCHAR,
                    tfa_status VARCHAR,
                    first_activation_date TIMESTAMPTZ,
                    expiration_date TIMESTAMPTZ,
                    telephone VARCHAR,
                    indicatif VARCHAR,
                    date_modif_tel TIMESTAMPTZ,
                    numero_pi VARCHAR,
                    expiration_doc DATE,
                    emission_doc DATE,
                    type_doc VARCHAR,
                    user_uuid VARCHAR,
                    identity_verification_mode VARCHAR,
                    identity_verification_status VARCHAR,
                    identity_verification_result VARCHAR,
                    id_identity_verification_proof VARCHAR,
                    identity_verification_date TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ,
                    operateur VARCHAR,
                    PRIMARY KEY (id)
                );
            """)
            
            logger.info("Table user_data_with_operators créée/vérifiée dans DuckDB")
            
            # Charger les données
            conn.register('temp_df', df)
            conn.execute("""
                INSERT OR REPLACE INTO user_data_with_operators 
                SELECT * FROM temp_df;
            """)
            
            # Mettre à jour le log de synchronisation
            conn.execute("""
                INSERT OR REPLACE INTO etl_sync_log (table_name, last_sync, records_synced, etl_duration_seconds)
                VALUES ('user_data_with_operators', ?, ?, ?);
            """, (etl_start_time, len(df), int((datetime.now(timezone.utc) - etl_start_time).total_seconds())))
            
            conn.close()
            
            logger.info(f"{len(df)} enregistrements synchronisés avec succès dans DuckDB")
            return len(df)
            
        except duckdb.IOException as e:
            if "Conflicting lock" in str(e) and attempt < max_retries - 1:
                wait_time = 2 ** attempt + 5  # Exponential backoff + base wait
                logger.warning(f"DuckDB verrouillé pour l'écriture, tentative {attempt + 1}/{max_retries}, attente {wait_time}s...")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"Erreur lors du chargement dans DuckDB: {e}")
                return 0
        except Exception as e:
            logger.error(f"Erreur lors du chargement dans DuckDB: {e}")
            return 0
    
    logger.error("Échec du chargement après toutes les tentatives")
    return 0

def run_etl():
    """Exécute le pipeline ETL complet."""
    etl_start = datetime.now(timezone.utc)
    logger.info("=== DÉBUT ETL PostgreSQL -> DuckDB ===")
    
    try:
        # 1. Récupérer le timestamp de dernière synchronisation
        last_sync = get_last_sync_timestamp()
        
        # 2. Extraire les données depuis PostgreSQL
        user_data, operator_data = extract_user_data_from_postgres(last_sync)
        
        if user_data.empty:
            logger.info("Aucune nouvelle donnée utilisateur à traiter.")
            return
        
        # 3. Joindre les données utilisateur avec les opérateurs
        enriched_data = join_operator_data(user_data, operator_data)
        
        # 4. Charger dans DuckDB
        records_loaded = load_to_duckdb(enriched_data, etl_start)
        
        duration = (datetime.now(timezone.utc) - etl_start).total_seconds()
        logger.info(f"=== ETL TERMINÉ - {records_loaded} enregistrements en {duration:.2f}s ===")
        
    except Exception as e:
        logger.error(f"=== ERREUR ETL: {e} ===")
        raise

if __name__ == "__main__":
    run_etl()