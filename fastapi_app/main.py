from fastapi import FastAPI, Query, HTTPException
import duckdb
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from pydantic import BaseModel

app = FastAPI(title="Operators Management API", version="1.0.0")

# Configuration CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Pour la démo, autorise tout. En prod, restreins !
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DUCKDB_FILE = "/opt/airflow/olap_data/olap.duckdb"  # Chemin du volume partagé

# Modèles Pydantic pour les opérateurs télécom
class OperatorResponse(BaseModel):
    id: int
    first_name: Optional[str]
    birth_name: Optional[str]
    middle_name: Optional[str]
    last_name: Optional[str]
    sex: Optional[str]
    birth_date: Optional[str]
    cogville: Optional[str]
    cogpays: Optional[str]
    birth_city: Optional[str]
    birth_country: Optional[str]
    email: Optional[str]
    created_date: Optional[str]
    archived_date: Optional[str]
    uuid: Optional[str]
    id_ccu: Optional[str]
    subscription_channel: Optional[str]
    verification_mode: Optional[str]
    verification_date: Optional[str]
    user_status: Optional[str]
    two_fa_status: Optional[str]
    first_activation_date: Optional[str]
    expiration_date: Optional[str]
    telephone: Optional[str]
    indicatif: Optional[str]
    date_modif_tel: Optional[str]
    date_modf_tel: Optional[str]
    numero_pi: Optional[str]
    expiration: Optional[str]
    emission: Optional[str]
    type: Optional[str]
    user_uuid: Optional[str]
    identity_verification_mode: Optional[str]
    identity_verification_status: Optional[str]
    identity_verification_result: Optional[str]
    id_identity_verification_proof: Optional[str]
    identity_verification_date: Optional[str]
    operateur: Optional[str]

class OperatorStats(BaseModel):
    total_operators: int
    active_operators: int
    by_operateur: List[dict]
    by_status: List[dict]
    by_2fa: List[dict]
    by_verification: List[dict]


@app.get("/operators", response_model=List[OperatorResponse])
def get_operators(
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    operateur: Optional[str] = Query(None),
    user_status: Optional[str] = Query(None),
    two_fa_status: Optional[str] = Query(None),
    subscription_channel: Optional[str] = Query(None)
):
    """Récupère la liste des opérateurs télécom avec filtres optionnels."""
    try:
        con = duckdb.connect(DUCKDB_FILE, read_only=True)
        
        # Construction de la requête avec filtres
        where_clauses = []
        params = []
        
        if operateur:
            where_clauses.append("operateur = ?")
            params.append(operateur)
        if user_status:
            where_clauses.append("user_status = ?")
            params.append(user_status)
        if two_fa_status:
            where_clauses.append("two_fa_status = ?")
            params.append(two_fa_status)
        if subscription_channel:
            where_clauses.append("subscription_channel = ?")
            params.append(subscription_channel)
        
        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        
        query = f"""
            SELECT * FROM operators 
            {where_sql}
            ORDER BY id 
            LIMIT ? OFFSET ?
        """
        
        params.extend([limit, offset])
        rows = con.execute(query, params).fetchall()
        
        # Récupérer les noms des colonnes
        columns = [desc[0] for desc in con.description]
        
        con.close()
        
        # Convertir en dictionnaires
        operators = []
        for row in rows:
            operator_dict = dict(zip(columns, row))
            # Convertir les dates en strings
            for date_field in ['birth_date', 'created_date', 'archived_date', 'verification_date', 
                             'first_activation_date', 'expiration_date', 'date_modif_tel', 
                             'date_modf_tel', 'expiration', 'emission', 'identity_verification_date']:
                if operator_dict.get(date_field):
                    operator_dict[date_field] = str(operator_dict[date_field])
            operators.append(operator_dict)
        
        return operators
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur base de données: {str(e)}")

@app.get("/operators/stats", response_model=OperatorStats)
def get_operators_stats():
    """Récupère les statistiques des opérateurs télécom."""
    try:
        con = duckdb.connect(DUCKDB_FILE, read_only=True)
        
        # Statistiques générales
        stats_query = """
            SELECT 
                COUNT(*) as total_operators,
                COUNT(CASE WHEN user_status = 'ACTIVE' THEN 1 END) as active_operators
            FROM operators
        """
        
        general_stats = con.execute(stats_query).fetchone()
        
        # Répartition par opérateur
        operateur_query = """
            SELECT 
                operateur,
                COUNT(*) as count,
                COUNT(CASE WHEN user_status = 'ACTIVE' THEN 1 END) as active_count
            FROM operators
            GROUP BY operateur
            ORDER BY count DESC
        """
        
        operateur_stats = con.execute(operateur_query).fetchall()
        
        # Répartition par statut utilisateur
        status_query = """
            SELECT 
                user_status,
                COUNT(*) as count
            FROM operators
            GROUP BY user_status
            ORDER BY count DESC
        """
        
        status_stats = con.execute(status_query).fetchall()
        
        # Répartition par statut 2FA
        fa2_query = """
            SELECT 
                two_fa_status,
                COUNT(*) as count
            FROM operators
            GROUP BY two_fa_status
            ORDER BY count DESC
        """
        
        fa2_stats = con.execute(fa2_query).fetchall()
        
        # Répartition par statut de vérification
        verification_query = """
            SELECT 
                identity_verification_status,
                COUNT(*) as count
            FROM operators
            GROUP BY identity_verification_status
            ORDER BY count DESC
        """
        
        verification_stats = con.execute(verification_query).fetchall()
        
        con.close()
        
        return {
            "total_operators": general_stats[0],
            "active_operators": general_stats[1],
            "by_operateur": [
                {"operateur": row[0], "count": row[1], "active_count": row[2]}
                for row in operateur_stats
            ],
            "by_status": [
                {"status": row[0], "count": row[1]}
                for row in status_stats
            ],
            "by_2fa": [
                {"status": row[0], "count": row[1]}
                for row in fa2_stats
            ],
            "by_verification": [
                {"status": row[0], "count": row[1]}
                for row in verification_stats
            ]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur base de données: {str(e)}")

# Endpoint pour les valeurs de filtres disponibles
@app.get("/operators-filters")
def get_filter_options():
    """Récupère les options disponibles pour les filtres télécom."""
    try:
        con = duckdb.connect(DUCKDB_FILE, read_only=True)
        
        # Opérateurs disponibles
        operateurs = con.execute("SELECT DISTINCT operateur FROM operators WHERE operateur IS NOT NULL ORDER BY operateur").fetchall()
        
        # Statuts utilisateur disponibles
        user_statuses = con.execute("SELECT DISTINCT user_status FROM operators WHERE user_status IS NOT NULL ORDER BY user_status").fetchall()
        
        # Statuts 2FA disponibles
        fa2_statuses = con.execute("SELECT DISTINCT two_fa_status FROM operators WHERE two_fa_status IS NOT NULL ORDER BY two_fa_status").fetchall()
        
        # Canaux de souscription disponibles
        channels = con.execute("SELECT DISTINCT subscription_channel FROM operators WHERE subscription_channel IS NOT NULL ORDER BY subscription_channel").fetchall()
        
        # Modes de vérification disponibles
        verification_modes = con.execute("SELECT DISTINCT verification_mode FROM operators WHERE verification_mode IS NOT NULL ORDER BY verification_mode").fetchall()
        
        con.close()
        
        return {
            "operateurs": [row[0] for row in operateurs],
            "user_statuses": [row[0] for row in user_statuses],
            "two_fa_statuses": [row[0] for row in fa2_statuses],
            "subscription_channels": [row[0] for row in channels],
            "verification_modes": [row[0] for row in verification_modes]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur base de données: {str(e)}")

@app.get("/operators/{operator_id}", response_model=OperatorResponse)
def get_operator(operator_id: int):
    """Récupère un opérateur spécifique par son ID."""
    try:
        con = duckdb.connect(DUCKDB_FILE, read_only=True)
        
        row = con.execute("SELECT * FROM operators WHERE id = ?", [operator_id]).fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail="Opérateur non trouvé")
        
        # Récupérer les noms des colonnes
        columns = [desc[0] for desc in con.description]
        con.close()
        
        # Convertir en dictionnaire
        operator_dict = dict(zip(columns, row))
        # Convertir les dates en strings
        for date_field in ['birth_date', 'created_date', 'archived_date', 'verification_date', 
                         'first_activation_date', 'expiration_date', 'date_modif_tel', 
                         'date_modf_tel', 'expiration', 'emission', 'identity_verification_date']:
            if operator_dict.get(date_field):
                operator_dict[date_field] = str(operator_dict[date_field])
        
        return operator_dict
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur base de données: {str(e)}")