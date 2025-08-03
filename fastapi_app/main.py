from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import duckdb
import time
from datetime import datetime
from typing import Optional, List, Dict, Any
import json

app = FastAPI(
    title="User Data Analytics API",
    description="API pour l'analyse des données utilisateur avec enrichissement opérateur télécom",
    version="2.0.0"
)

# Configuration CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En production, restreindre aux domaines autorisés
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DUCKDB_FILE = "/opt/airflow/olap.duckdb"

def get_db_connection():
    """Crée une connexion à DuckDB avec gestion d'erreur et fermeture automatique."""
    try:
        conn = duckdb.connect(DUCKDB_FILE, read_only=True)
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur de connexion à la base: {str(e)}")

def execute_query(query, params=None):
    """Exécute une requête avec gestion automatique de la connexion."""
    conn = None
    try:
        conn = get_db_connection()
        if params:
            result = conn.execute(query, params).fetchall()
        else:
            result = conn.execute(query).fetchall()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur d'exécution de requête: {str(e)}")
    finally:
        if conn:
            conn.close()

@app.get("/")
def root():
    """Page d'accueil de l'API."""
    return {
        "message": "User Data Analytics API",
        "version": "2.0.0",
        "description": "API pour l'analyse des données utilisateur avec enrichissement opérateur télécom",
        "endpoints": {
            "users": "/users - Liste des utilisateurs avec filtres",
            "users/stats": "/users/stats - Statistiques globales",
            "users/operators": "/users/operators - Répartition par opérateur",
            "users/countries": "/users/countries - Répartition par pays",
            "users/channels": "/users/channels - Répartition par canal d'inscription",
            "sync/status": "/sync/status - Statut de la dernière synchronisation"
        }
    }

@app.get("/users")
def get_users(
    limit: int = Query(100, ge=1, le=1000, description="Nombre maximum d'utilisateurs à retourner"),
    offset: int = Query(0, ge=0, description="Décalage pour la pagination"),
    operateur: Optional[str] = Query(None, description="Filtrer par opérateur télécom"),
    pays: Optional[str] = Query(None, description="Filtrer par pays de naissance"),
    canal: Optional[str] = Query(None, description="Filtrer par canal d'inscription"),
    statut: Optional[str] = Query(None, description="Filtrer par statut utilisateur")
):
    """Récupère la liste des utilisateurs avec filtres optionnels."""
    try:
        conn = get_db_connection()
        
        # Construction de la requête avec filtres
        where_conditions = []
        params = []
        
        if operateur:
            where_conditions.append("operateur = ?")
            params.append(operateur)
        
        if pays:
            where_conditions.append("birth_country = ?")
            params.append(pays)
            
        if canal:
            where_conditions.append("subscription_channel = ?")
            params.append(canal)
            
        if statut:
            where_conditions.append("user_status = ?")
            params.append(statut)
        
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        query = f"""
            SELECT 
                id, first_name, last_name, email, telephone, operateur,
                birth_country, subscription_channel, user_status, created_date,
                verification_date, first_activation_date
            FROM user_data_with_operators 
            {where_clause}
            ORDER BY created_date DESC 
            LIMIT ? OFFSET ?
        """
        
        params.extend([limit, offset])
        
        rows = conn.execute(query, params).fetchall()
        columns = [desc[0] for desc in conn.description]
        
        # Compter le total pour la pagination
        count_query = f"SELECT COUNT(*) FROM user_data_with_operators {where_clause}"
        total = conn.execute(count_query, params[:-2]).fetchone()[0]  # Exclure limit et offset
        
        conn.close()
        
        users = []
        for row in rows:
            user = dict(zip(columns, row))
            # Convertir les timestamps en strings
            for key, value in user.items():
                if isinstance(value, datetime):
                    user[key] = value.isoformat()
            users.append(user)
        
        return {
            "users": users,
            "pagination": {
                "total": total,
                "limit": limit,
                "offset": offset,
                "has_more": offset + limit < total
            },
            "filters": {
                "operateur": operateur,
                "pays": pays,
                "canal": canal,
                "statut": statut
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/users/stats")
def get_user_stats():
    """Retourne les statistiques globales des utilisateurs."""
    try:
        conn = get_db_connection()
        
        stats = {}
        
        # Total utilisateurs
        stats['total_users'] = conn.execute("SELECT COUNT(*) FROM user_data_with_operators").fetchone()[0]
        
        # Utilisateurs vérifiés
        stats['verified_users'] = conn.execute(
            "SELECT COUNT(*) FROM user_data_with_operators WHERE user_status = 'verified'"
        ).fetchone()[0]
        
        # Nouveaux utilisateurs aujourd'hui
        stats['new_users_today'] = conn.execute(
            "SELECT COUNT(*) FROM user_data_with_operators WHERE DATE(created_date) = CURRENT_DATE"
        ).fetchone()[0]
        
        # Nouveaux utilisateurs cette semaine
        stats['new_users_week'] = conn.execute(
            "SELECT COUNT(*) FROM user_data_with_operators WHERE created_date >= CURRENT_DATE - INTERVAL 7 DAY"
        ).fetchone()[0]
        
        # Répartition par sexe
        gender_stats = conn.execute(
            "SELECT sex, COUNT(*) FROM user_data_with_operators GROUP BY sex"
        ).fetchall()
        stats['gender_distribution'] = {row[0] or 'Non spécifié': row[1] for row in gender_stats}
        
        # Répartition par statut 2FA
        tfa_stats = conn.execute(
            "SELECT tfa_status, COUNT(*) FROM user_data_with_operators GROUP BY tfa_status"
        ).fetchall()
        stats['tfa_distribution'] = {row[0] or 'Non activé': row[1] for row in tfa_stats}
        
        conn.close()
        
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/users/operators")
def get_users_by_operator():
    """Retourne la répartition des utilisateurs par opérateur télécom."""
    try:
        conn = get_db_connection()
        
        rows = conn.execute("""
            SELECT 
                operateur, 
                COUNT(*) as count,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
            FROM user_data_with_operators 
            WHERE operateur IS NOT NULL
            GROUP BY operateur 
            ORDER BY count DESC
        """).fetchall()
        
        conn.close()
        
        return [
            {
                "operateur": row[0] or 'Non identifié',
                "count": row[1],
                "percentage": round(row[2], 2)
            }
            for row in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/users/countries")
def get_users_by_country():
    """Retourne la répartition des utilisateurs par pays de naissance."""
    try:
        conn = get_db_connection()
        
        rows = conn.execute("""
            SELECT 
                birth_country, 
                COUNT(*) as count,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
            FROM user_data_with_operators 
            GROUP BY birth_country 
            ORDER BY count DESC
            LIMIT 20
        """).fetchall()
        
        conn.close()
        
        return [
            {
                "country": row[0] or 'Non spécifié',
                "count": row[1],
                "percentage": round(row[2], 2)
            }
            for row in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/users/channels")
def get_users_by_channel():
    """Retourne la répartition des utilisateurs par canal d'inscription."""
    try:
        conn = get_db_connection()
        
        rows = conn.execute("""
            SELECT 
                subscription_channel, 
                COUNT(*) as count,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
            FROM user_data_with_operators 
            GROUP BY subscription_channel 
            ORDER BY count DESC
        """).fetchall()
        
        conn.close()
        
        return [
            {
                "channel": row[0] or 'Non spécifié',
                "count": row[1],
                "percentage": round(row[2], 2)
            }
            for row in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/sync/status")
def get_sync_status():
    """Retourne l'état de la dernière synchronisation ETL."""
    try:
        conn = get_db_connection()
        
        # Vérifier si la table de sync existe
        table_exists = conn.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name = 'etl_sync_log'
        """).fetchone()[0]
        
        if table_exists == 0:
            return {
                "status": "no_sync",
                "message": "Aucune synchronisation n'a encore été effectuée"
            }
        
        # Récupérer les informations de synchronisation
        sync_info = conn.execute("""
            SELECT 
                last_sync, 
                records_synced, 
                etl_duration_seconds
            FROM etl_sync_log 
            WHERE table_name = 'user_data_with_operators'
        """).fetchone()
        
        if not sync_info:
            return {
                "status": "no_sync",
                "message": "Aucune synchronisation trouvée pour les données utilisateur"
            }
        
        # Total d'enregistrements dans DuckDB
        total_records = conn.execute(
            "SELECT COUNT(*) FROM user_data_with_operators"
        ).fetchone()[0]
        
        conn.close()
        
        last_sync, records_synced, duration = sync_info
        
        # Calculer le temps depuis la dernière sync
        if isinstance(last_sync, str):
            last_sync_dt = datetime.fromisoformat(last_sync.replace('Z', '+00:00'))
        else:
            last_sync_dt = last_sync
        
        time_since_sync = datetime.now() - last_sync_dt.replace(tzinfo=None)
        
        return {
            "status": "synced",
            "last_sync": last_sync_dt.isoformat(),
            "records_last_sync": records_synced,
            "total_records": total_records,
            "etl_duration_seconds": duration,
            "time_since_last_sync": {
                "hours": time_since_sync.total_seconds() // 3600,
                "minutes": (time_since_sync.total_seconds() % 3600) // 60,
                "seconds": time_since_sync.total_seconds() % 60
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stream/stats")
def stream_live_stats():
    """Stream en temps réel des statistiques."""
    def event_stream():
        while True:
            try:
                conn = get_db_connection()
                
                # Statistiques de base
                total = conn.execute("SELECT COUNT(*) FROM user_data_with_operators").fetchone()[0]
                verified = conn.execute(
                    "SELECT COUNT(*) FROM user_data_with_operators WHERE user_status = 'verified'"
                ).fetchone()[0]
                
                conn.close()
                
                data = {
                    "timestamp": datetime.now().isoformat(),
                    "total_users": total,
                    "verified_users": verified,
                    "verification_rate": round((verified / total * 100) if total > 0 else 0, 2)
                }
                
                yield f"data: {json.dumps(data)}\n\n"
                time.sleep(5)  # Mise à jour toutes les 5 secondes
                
            except Exception as e:
                yield f"data: {json.dumps({'error': str(e)})}\n\n"
                time.sleep(10)
    
    return StreamingResponse(event_stream(), media_type="text/event-stream")