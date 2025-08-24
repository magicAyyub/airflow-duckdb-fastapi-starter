# Guide de Déploiement: Production Database Integration

## 📋 Vue d'ensemble

Ce guide explique comment adapter votre système actuel pour se connecter à la vraie base de données de production, en tenant compte des contraintes suivantes :
- **Accès lecture seule** à la base de données de production
- **Pas de droits d'administration** sur la base de données
- **Déploiement sur VPS** avec accès distant à la base
- **Conservation du système ETL existant**

## 🏗️ Architecture de Production

```
[Base de Production] ---(lecture seule)---> [VPS]
                                             ├── PostgreSQL (local/cache)
                                             ├── DuckDB (OLAP)
                                             ├── Airflow (ETL)
                                             ├── FastAPI (API)
                                             └── Frontend (Dashboard)
```

## 📝 Prérequis

### Ce que vous devez demander à votre manager :

1. **Accès VPS** :
   - Adresse IP du VPS
   - Utilisateur et mot de passe SSH
   - Ports disponibles (8000, 8080, 3000)

2. **Accès Base de Données** :
   - Host/IP de la base de production
   - Port (généralement 5432 pour PostgreSQL)
   - Nom de la base de données
   - Utilisateur en lecture seule
   - Mot de passe
   - Nom de la table contenant les données utilisateur
   - Existence d'une colonne `updated_at` ou équivalent

3. **Informations sur la Structure** :
   - Schéma exact de la table de production
   - Noms des colonnes (pour mapper avec votre structure CSV)
   - Présence de données MAJNUM ou équivalent

## 🔧 Étapes de Déploiement

### Étape 1: Préparer les Configurations

#### 1.1 Créer le fichier de configuration production

```bash
# Créer le fichier de configuration
touch production.env
```

```env
# production.env
# ======================
# Configuration Base de Production
# ======================
PROD_PGHOST=<IP_BASE_PRODUCTION>
PROD_PGPORT=5432
PROD_PGUSER=<UTILISATEUR_LECTURE_SEULE>
PROD_PGPASSWORD=<MOT_DE_PASSE>
PROD_PGDATABASE=<NOM_BASE_PRODUCTION>
PROD_TABLE_NAME=<NOM_TABLE_USERS>

# ======================
# Configuration Cache Local (VPS)
# ======================
CACHE_PGHOST=localhost
CACHE_PGPORT=5433
CACHE_PGUSER=cache_user
CACHE_PGPASSWORD=cache_password
CACHE_PGDATABASE=cache_db

# ======================
# Configuration DuckDB
# ======================
DUCKDB_FILE=/opt/data/production.duckdb

# ======================
# Configuration Airflow
# ======================
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://cache_user:cache_password@localhost:5433/airflow

# ======================
# Mapping des Colonnes
# ======================
# Si les noms de colonnes diffèrent de votre structure
COLUMN_MAPPING='{
  "user_first_name": "first_name",
  "user_last_name": "last_name",
  "phone_number": "telephone",
  "modification_date": "updated_at"
}'
```

### Étape 2: Adapter l'ETL pour la Production

#### 2.1 Créer un ETL hybride (Production → Cache → DuckDB)

```python
# airflow/etl/etl_production_to_cache.py
import os
import psycopg2
import pandas as pd
from datetime import datetime, timezone
import logging
import json

# Configuration
PROD_HOST = os.getenv("PROD_PGHOST")
PROD_PORT = os.getenv("PROD_PGPORT", "5432")
PROD_USER = os.getenv("PROD_PGUSER")
PROD_PASSWORD = os.getenv("PROD_PGPASSWORD")
PROD_DB = os.getenv("PROD_PGDATABASE")
PROD_TABLE = os.getenv("PROD_TABLE_NAME", "user_data")

CACHE_HOST = os.getenv("CACHE_PGHOST", "localhost")
CACHE_PORT = os.getenv("CACHE_PGPORT", "5433")
CACHE_USER = os.getenv("CACHE_PGUSER", "cache_user")
CACHE_PASSWORD = os.getenv("CACHE_PGPASSWORD")
CACHE_DB = os.getenv("CACHE_PGDATABASE", "cache_db")

# Mapping des colonnes si nécessaire
COLUMN_MAPPING = json.loads(os.getenv("COLUMN_MAPPING", "{}"))

def extract_from_production(last_sync=None):
    """Extrait les données depuis la base de production (lecture seule)"""
    try:
        logger.info("Connexion à la base de production...")
        conn = psycopg2.connect(
            host=PROD_HOST,
            port=PROD_PORT,
            user=PROD_USER,
            password=PROD_PASSWORD,
            dbname=PROD_DB
        )
        
        # Adapter la requête selon la structure de votre base
        if last_sync:
            # Utiliser updated_at ou équivalent pour récupérer seulement les modifications
            query = f"""
                SELECT * FROM {PROD_TABLE} 
                WHERE updated_at > %s OR created_date > %s
                ORDER BY id;
            """
            df = pd.read_sql_query(query, conn, params=[last_sync, last_sync])
        else:
            # Premier import complet
            query = f"SELECT * FROM {PROD_TABLE} ORDER BY id;"
            df = pd.read_sql_query(query, conn)
        
        conn.close()
        
        # Appliquer le mapping des colonnes si nécessaire
        if COLUMN_MAPPING:
            df = df.rename(columns=COLUMN_MAPPING)
        
        logger.info(f"Extraction terminée: {len(df)} enregistrements")
        return df
        
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction depuis la production: {e}")
        return pd.DataFrame()

def load_to_cache(df):
    """Charge les données dans le cache local"""
    try:
        logger.info("Chargement dans le cache local...")
        conn = psycopg2.connect(
            host=CACHE_HOST,
            port=CACHE_PORT,
            user=CACHE_USER,
            password=CACHE_PASSWORD,
            dbname=CACHE_DB
        )
        
        # Utiliser votre structure existante
        df.to_sql('user_data', conn, if_exists='replace', index=False, method='multi')
        
        conn.close()
        logger.info(f"Cache mis à jour avec {len(df)} enregistrements")
        return True
        
    except Exception as e:
        logger.error(f"Erreur lors du chargement en cache: {e}")
        return False
```

#### 2.2 Modifier l'ETL existant pour utiliser le cache

```python
# Dans airflow/etl/etl_postgres_to_duckdb.py
# Modifier la fonction extract_user_data_from_postgres

def extract_user_data_from_postgres(last_sync=None):
    """Extrait les données utilisateur depuis le cache local."""
    try:
        logger.info("Connexion au cache PostgreSQL...")
        conn = psycopg2.connect(
            host=CACHE_HOST,  # localhost sur le VPS
            port=CACHE_PORT,
            user=CACHE_USER,
            password=CACHE_PASSWORD,
            dbname=CACHE_DB
        )
        
        # Reste du code identique...
```

### Étape 3: Configuration Docker pour Production

#### 3.1 Nouveau docker-compose.prod.yml

```yaml
# docker-compose.prod.yml
version: '3.8'

services:
  cache-postgres:
    image: postgres:14
    environment:
      POSTGRES_USER: cache_user
      POSTGRES_PASSWORD: cache_password
      POSTGRES_DB: cache_db
    volumes:
      - cache_postgres_data:/var/lib/postgresql/data
      - ./postgres/init-cache.sql:/docker-entrypoint-initdb.d/init.sql
    ports:
      - "5433:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U cache_user -d cache_db"]
      interval: 30s
      timeout: 10s
      retries: 3

  airflow-webserver:
    build: ./airflow
    depends_on:
      cache-postgres:
        condition: service_healthy
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://cache_user:cache_password@cache-postgres:5432/airflow
      # Variables de production
      PROD_PGHOST: ${PROD_PGHOST}
      PROD_PGPORT: ${PROD_PGPORT}
      PROD_PGUSER: ${PROD_PGUSER}
      PROD_PGPASSWORD: ${PROD_PGPASSWORD}
      PROD_PGDATABASE: ${PROD_PGDATABASE}
      PROD_TABLE_NAME: ${PROD_TABLE_NAME}
      # Variables de cache
      CACHE_PGHOST: cache-postgres
      CACHE_PGPORT: 5432
      CACHE_PGUSER: cache_user
      CACHE_PGPASSWORD: cache_password
      CACHE_PGDATABASE: cache_db
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/etl:/opt/airflow/etl
      - ./production_data:/opt/airflow
    command: >
      bash -c "
        airflow db init &&
        airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com &&
        airflow webserver
      "
    ports:
      - "8080:8080"

  airflow-scheduler:
    build: ./airflow
    depends_on:
      cache-postgres:
        condition: service_healthy
      airflow-webserver:
        condition: service_started
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://cache_user:cache_password@cache-postgres:5432/airflow
      # Mêmes variables que webserver
      PROD_PGHOST: ${PROD_PGHOST}
      PROD_PGPORT: ${PROD_PGPORT}
      PROD_PGUSER: ${PROD_PGUSER}
      PROD_PGPASSWORD: ${PROD_PGPASSWORD}
      PROD_PGDATABASE: ${PROD_PGDATABASE}
      CACHE_PGHOST: cache-postgres
      CACHE_PGPORT: 5432
      CACHE_PGUSER: cache_user
      CACHE_PGPASSWORD: cache_password
      CACHE_PGDATABASE: cache_db
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/etl:/opt/airflow/etl
      - ./production_data:/opt/airflow
    command: airflow scheduler

  fastapi:
    build: ./fastapi_app
    ports:
      - "8000:8000"
    volumes:
      - ./production_data:/opt/airflow
    depends_on:
      - airflow-webserver
    environment:
      DUCKDB_FILE: /opt/airflow/production.duckdb

  frontend:
    build: ./frontend
    ports:
      - "3000:3000"
    environment:
      NEXT_PUBLIC_BACKEND_URL: http://localhost:8000
    depends_on:
      - fastapi

volumes:
  cache_postgres_data:
```

### Étape 4: Nouveaux DAGs pour la Production

#### 4.1 DAG de synchronisation Production → Cache

```python
# airflow/dags/sync_production_to_cache.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append('/opt/airflow/etl')
from etl_production_to_cache import extract_from_production, load_to_cache

def sync_production_data():
    """Synchronise les données de production vers le cache"""
    # Récupérer la dernière synchronisation
    last_sync = get_last_sync()  # À implémenter
    
    # Extraire de la production
    df = extract_from_production(last_sync)
    
    if not df.empty:
        # Charger en cache
        success = load_to_cache(df)
        if success:
            # Mettre à jour le timestamp de sync
            update_sync_log()  # À implémenter
    
    return len(df)

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='sync_production_to_cache',
    default_args=default_args,
    description='Synchronise les données de production vers le cache local',
    schedule_interval='@hourly',  # Toutes les heures
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['production', 'sync', 'cache']
) as dag:

    sync_task = PythonOperator(
        task_id='sync_production_data',
        python_callable=sync_production_data,
    )
```

#### 4.2 Modifier le DAG ETL existant

```python
# airflow/dags/etl_cache_to_duckdb.py
# Copier le DAG existant et modifier pour utiliser le cache
# Changer schedule_interval pour s'exécuter après la sync
schedule_interval='*/20 * * * *'  # 5 minutes après la sync
```

### Étape 5: Scripts de Déploiement

#### 5.1 Script d'installation sur VPS

```bash
#!/bin/bash
# deploy.sh

set -e

echo "🚀 Déploiement sur VPS de Production"

# 1. Mettre à jour le système
sudo apt update && sudo apt upgrade -y

# 2. Installer Docker et Docker Compose
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER

# Installer Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# 3. Créer les répertoires
mkdir -p /opt/operator-dashboard
cd /opt/operator-dashboard

# 4. Cloner le repository (ou copier les fichiers)
# git clone <your-repo> .

# 5. Configurer les permissions
sudo chown -R $USER:$USER /opt/operator-dashboard

# 6. Copier la configuration de production
cp production.env .env

# 7. Créer les volumes de données
mkdir -p production_data

# 8. Démarrer les services
docker-compose -f docker-compose.prod.yml up -d

echo "✅ Déploiement terminé!"
echo "📊 Dashboard disponible sur: http://<VPS-IP>:3000"
echo "🔧 Airflow disponible sur: http://<VPS-IP>:8080"
echo "🔌 API disponible sur: http://<VPS-IP>:8000"
```

#### 5.2 Script de test de connexion

```bash
#!/bin/bash
# test-connection.sh

echo "🔍 Test de connexion à la base de production"

# Test de connectivité réseau
nc -zv $PROD_PGHOST $PROD_PGPORT

# Test de connexion PostgreSQL
PGPASSWORD=$PROD_PGPASSWORD psql -h $PROD_PGHOST -p $PROD_PGPORT -U $PROD_PGUSER -d $PROD_PGDATABASE -c "SELECT version();"

# Test de lecture de la table
PGPASSWORD=$PROD_PGPASSWORD psql -h $PROD_PGHOST -p $PROD_PGPORT -U $PROD_PGUSER -d $PROD_PGDATABASE -c "SELECT COUNT(*) FROM $PROD_TABLE_NAME;"

echo "✅ Tests de connexion terminés"
```

### Étape 6: Monitoring et Maintenance

#### 6.1 Surveillance des performances

```python
# monitoring/check_sync.py
import psycopg2
import time
from datetime import datetime, timedelta

def check_sync_health():
    """Vérifie la santé de la synchronisation"""
    
    # Vérifier le lag entre production et cache
    prod_count = get_count_production()
    cache_count = get_count_cache()
    
    lag = abs(prod_count - cache_count)
    
    if lag > 100:  # Seuil d'alerte
        send_alert(f"Lag détecté: {lag} enregistrements")
    
    # Vérifier la fraîcheur des données
    last_sync = get_last_sync_time()
    if last_sync < datetime.now() - timedelta(minutes=30):
        send_alert("Synchronisation en retard")
```

#### 6.2 Script de backup

```bash
#!/bin/bash
# backup.sh

# Backup du cache PostgreSQL
pg_dump -h localhost -p 5433 -U cache_user cache_db > backup_cache_$(date +%Y%m%d_%H%M%S).sql

# Backup de DuckDB
cp /opt/data/production.duckdb backup_duckdb_$(date +%Y%m%d_%H%M%S).duckdb

# Nettoyer les anciens backups (garder 7 jours)
find . -name "backup_*" -mtime +7 -delete
```

## 🚨 Points d'Attention

### Sécurité
- [ ] Changer tous les mots de passe par défaut
- [ ] Configurer le firewall VPS (ufw)
- [ ] Utiliser des certificats SSL en production
- [ ] Limiter l'accès SSH par clé

### Performance
- [ ] Indexer les colonnes de filtrage dans le cache
- [ ] Optimiser les requêtes de synchronisation
- [ ] Surveiller l'espace disque DuckDB
- [ ] Mettre en place des alertes

### Robustesse
- [ ] Tester la reprise après panne
- [ ] Valider la cohérence des données
- [ ] Documenter les procédures de recovery
- [ ] Former l'équipe sur les opérations

## 📞 Support et Troubleshooting

### Commandes utiles

```bash
# Vérifier les logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f fastapi

# Redémarrer un service
docker-compose restart airflow-scheduler

# Vérifier l'état des services
docker-compose ps

# Accéder à un conteneur
docker-compose exec cache-postgres psql -U cache_user -d cache_db
```

### Problèmes courants

1. **Connexion à la production échoue**
   - Vérifier les credentials dans production.env
   - Tester la connectivité réseau
   - Vérifier les permissions de l'utilisateur DB

2. **Synchronisation lente**
   - Optimiser les requêtes avec LIMIT
   - Ajouter des index sur updated_at
   - Augmenter la fréquence de sync

3. **Espace disque insuffisant**
   - Nettoyer les anciens logs
   - Optimiser la taille de DuckDB
   - Mettre en place la rotation

---

Ce guide vous donne une feuille de route complète pour passer en production. Adaptez les configurations selon les spécificités de votre environnement réel.
