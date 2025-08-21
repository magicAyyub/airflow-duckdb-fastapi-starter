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

def transform_to_csv_structure(rows, columns):
    """Transform query results to match old CSV structure with exact column names"""
    if not rows:
        return []
    
    # Create mapping from database columns to CSV columns  
    column_mapping = {
        'first_name': 'FIRST_NAME',
        'birth_name': 'BIRTH_NAME', 
        'middle_name': 'MIDDLE_NAME',
        'last_name': 'LAST_NAME',
        'sex': 'SEX',
        'birth_date': 'BIRTH_DATE',
        'cogville': 'COGVILLE',
        'cogpays': 'COGPAYS',
        'birth_city': 'BIRTH_CITY',
        'birth_country': 'BIRTH_COUNTRY',
        'email': 'EMAIL',
        'created_date': 'CREATED_DATE',
        'uuid': 'UUID',
        'id_ccu': 'ID_CCU',
        'subscription_channel': 'SUBSCRIPTION_CHANNEL',
        'verification_mode': 'VERIFICATION_MODE',
        'verification_date': 'VERIFICATION_DATE',
        'user_status': 'USER_STATUS',
        'tfa_status': '2FA_STATUS',  # Special mapping: tfa_status -> 2FA_STATUS
        'first_activation_date': 'FIRST_ACTIVATION_DATE',
        'expiration_date': 'EXPIRATION_DATE',
        'telephone': 'TELEPHONE',
        'indicatif': 'INDICATIF',
        'date_modif_tel': 'DATE_MODIF_TEL',
        'numero_pi': 'Numero Pi',
        'expiration_doc': 'EXPIRATION',
        'emission_doc': 'EMISSION', 
        'type_doc': 'TYPE',
        'user_uuid': 'USER_UUID',
        'identity_verification_mode': 'IDENTITY_VERIFICATION_MODE',
        'identity_verification_status': 'IDENTITY_VERIFICATION_STATUS',
        'identity_verification_result': 'IDENTITY_VERIFICATION_RESULT',
        'id_identity_verification_proof': 'ID_IDENTITY_VERIFICATION_PROOF',
        'identity_verification_date': 'IDENTITY_VERIFICATION_DATE',
        'operateur': 'OPERATEUR'  # Operator data from ETL join
    }
    
    # Map column names
    mapped_columns = [column_mapping.get(col, col) for col in columns]
    
    # Convert rows to list of dicts with mapped column names
    result = []
    for row in rows:
        row_dict = {}
        for i, value in enumerate(row):
            if i < len(mapped_columns):
                row_dict[mapped_columns[i]] = value
        result.append(row_dict)
    
    return result

@app.get("/api/csv/stats")
def get_stats(type: str = Query("operators", description="Type of stats: operators, status, 2fa")):
    """Get statistics based on the specified type"""
    if type not in ["operators", "status", "2fa"]:
        raise HTTPException(status_code=400, detail="Invalid stats type. Use: operators, status, 2fa")
    
    try:
        conn = get_db_connection()
        
        # Build the appropriate query based on the type
        if type == 'operators':
            query = """
                SELECT operateur as name, COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM user_data_with_operators), 2) as value
                FROM user_data_with_operators
                WHERE operateur IS NOT NULL
                GROUP BY operateur
                ORDER BY count DESC
                LIMIT 5
            """
        elif type == 'status':
            query = """
                SELECT user_status as name, COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM user_data_with_operators), 2) as value
                FROM user_data_with_operators
                WHERE user_status IS NOT NULL
                GROUP BY user_status
                ORDER BY count DESC
            """
        elif type == '2fa':
            query = """
                SELECT tfa_status as name, COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM user_data_with_operators), 2) as value
                FROM user_data_with_operators
                WHERE tfa_status IS NOT NULL
                GROUP BY tfa_status
                ORDER BY count DESC
            """
        
        result = conn.execute(query).fetchall()
        conn.close()
        
        # Transform the results
        data = []
        for row in result:
            data.append({
                "name": row[0] or "Non défini",
                "value": row[2]
            })
        
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/csv/filter-options")
def get_filter_options():
    """Get filter options for the UI"""
    try:
        conn = get_db_connection()
        
        # Get distinct user statuses
        statuts_query = """
            SELECT DISTINCT user_status as statut
            FROM user_data_with_operators
            WHERE user_status IS NOT NULL
            ORDER BY user_status
        """
        
        # Get distinct 2FA statuses
        fa_statuts_query = """
            SELECT DISTINCT tfa_status as fa_statut
            FROM user_data_with_operators
            WHERE tfa_status IS NOT NULL
            ORDER BY tfa_status
        """
        
        # Get distinct years from created_date
        annees_query = """
            SELECT DISTINCT EXTRACT(YEAR FROM created_date)::VARCHAR as annee
            FROM user_data_with_operators
            WHERE created_date IS NOT NULL
            ORDER BY annee
        """
        
        try:
            statuts = [row[0] for row in conn.execute(statuts_query).fetchall()]
        except Exception:
            statuts = []
        
        try:
            fa_statuts = [row[0] for row in conn.execute(fa_statuts_query).fetchall()]
        except Exception:
            fa_statuts = []
        
        try:
            annees = [row[0] for row in conn.execute(annees_query).fetchall()]
        except Exception:
            annees = []
        
        conn.close()
        
        return {
            "statuts": statuts,
            "fa_statuts": fa_statuts,
            "annees": annees
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/csv/data")
def get_data(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    statut: Optional[str] = None,
    fa_statut: Optional[str] = None,
    limite_type: Optional[str] = None,
    limite_valeur: Optional[float] = None,
    filtre_global: Optional[bool] = False,
    date_min: Optional[str] = None,
    date_max: Optional[str] = None,
    annee: Optional[str] = None
):
    """Get filtered data with pagination"""
    try:
        conn = get_db_connection()
        
        # Get total count
        total_count_query = "SELECT COUNT(*) as total FROM user_data_with_operators"
        total_count = conn.execute(total_count_query).fetchone()[0]
        
        # Get operator counts
        operator_count_query = """
            SELECT operateur, COUNT(*) as count
            FROM user_data_with_operators
            GROUP BY operateur
        """
        operator_counts = {row[0]: row[1] for row in conn.execute(operator_count_query).fetchall()}
        
        # Calculate global percentages
        global_percentages = {}
        for operateur, count in operator_counts.items():
            global_percentages[operateur] = round((count / total_count * 100), 2) if total_count > 0 else 0
        
        # Build filter conditions
        conditions = []
        
        if statut and statut != 'all':
            conditions.append(f"user_status = '{statut}'")
        
        if fa_statut and fa_statut != 'all':
            conditions.append(f"tfa_status = '{fa_statut}'")
        
        if date_min:
            conditions.append(f"created_date >= '{date_min}'")
        
        if date_max:
            from datetime import datetime, timedelta
            date_max_obj = datetime.strptime(date_max, "%Y-%m-%d")
            date_max_plus_one = (date_max_obj + timedelta(days=1)).strftime("%Y-%m-%d")
            conditions.append(f"created_date < '{date_max_plus_one}'")
        
        if annee and annee != 'all':
            conditions.append(f"EXTRACT(YEAR FROM created_date) = {annee}")
        
        # Build filtered query
        base_query = "SELECT * FROM user_data_with_operators"
        filtered_query = base_query
        if conditions:
            filtered_query += " WHERE " + " AND ".join(conditions)
        
        # Get filtered operator counts
        filtered_operator_query = f"""
            SELECT operateur, COUNT(*) as count
            FROM ({filtered_query}) as filtered_data
            GROUP BY operateur
        """
        filtered_operator_counts = {row[0]: row[1] for row in conn.execute(filtered_operator_query).fetchall()}
        
        # Get filtered total
        filtered_count_query = f"SELECT COUNT(*) as total FROM ({filtered_query}) as filtered_data"
        filtered_total = conn.execute(filtered_count_query).fetchone()[0]
        
        # Prepare data for all operators
        all_operators_data = []
        
        for operateur, filtered_count in filtered_operator_counts.items():
            global_percentage = global_percentages.get(operateur, 0)
            filtered_percentage = round((filtered_count / filtered_total * 100), 2) if filtered_total > 0 else 0
            
            all_operators_data.append({
                "id": operateur,
                "operateur": operateur,
                "nombre_in": filtered_count,
                "pourcentage_in": global_percentage,
                "pourcentage_filtre": filtered_percentage,
            })
        
        # Apply limit filter if needed
        if limite_type and limite_type != 'none' and limite_valeur is not None:
            filtered_data = []
            for item in all_operators_data:
                percentage_to_check = item["pourcentage_in"] if filtre_global else item["pourcentage_filtre"]
                
                if limite_type == 'lt' and percentage_to_check < float(limite_valeur):
                    filtered_data.append(item)
                elif limite_type == 'gt' and percentage_to_check > float(limite_valeur):
                    filtered_data.append(item)
            
            all_operators_data = filtered_data
        
        # Sort and paginate
        all_operators_data.sort(key=lambda x: x["nombre_in"], reverse=True)
        
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_data = all_operators_data[start_idx:end_idx]
        
        total_pages = (len(all_operators_data) + page_size - 1) // page_size if all_operators_data else 0
        
        conn.close()
        
        return {
            "data": paginated_data,
            "total_pages": total_pages,
            "total_count": len(all_operators_data),
            "is_filtered": len(conditions) > 0 or (limite_type and limite_type != 'none' and limite_valeur is not None)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/csv/head")
def get_head(n: int = Query(5, ge=1, le=100)):
    """Get the first n rows of the user data with CSV column structure"""
    try:
        conn = get_db_connection()
        
        # Select ALL columns to match the old CSV structure
        query = f"SELECT * FROM user_data_with_operators ORDER BY created_date DESC LIMIT {n}"
        
        rows = conn.execute(query).fetchall()
        columns = [desc[0] for desc in conn.description]
        
        conn.close()
        
        # Transform to CSV structure with proper column names
        transformed_data = transform_to_csv_structure(rows, columns)
        
        # Convert timestamps to strings
        for row in transformed_data:
            for key, value in row.items():
                if isinstance(value, datetime):
                    row[key] = value.isoformat()
        
        return {"data": transformed_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/csv/check")
def check_file():
    """Check if data exists in the database"""
    try:
        conn = get_db_connection()
        
        # Check if table exists and has data
        count_query = "SELECT COUNT(*) FROM user_data_with_operators"
        count = conn.execute(count_query).fetchone()[0]
        
        conn.close()
        
        return {
            "exists": count > 0,
            "count": count
        }
    except Exception as e:
        return {"exists": False, "error": str(e)}

@app.get("/api/csv/etl-status")
def get_etl_status():
    """Get ETL synchronization status and timing information"""
    try:
        conn = get_db_connection()
        
        # Get ETL sync log information
        etl_query = """
            SELECT 
                table_name,
                last_sync,
                records_synced,
                etl_duration_seconds
            FROM etl_sync_log 
            WHERE table_name = 'user_data_with_operators'
            ORDER BY last_sync DESC 
            LIMIT 1
        """
        
        etl_result = conn.execute(etl_query).fetchone()
        
        # Get total record count
        count_query = "SELECT COUNT(*) FROM user_data_with_operators"
        total_count = conn.execute(count_query).fetchone()[0]
        
        conn.close()
        
        if etl_result:
            return {
                "last_sync": etl_result[1].isoformat() if etl_result[1] else None,
                "records_synced": etl_result[2] or 0,
                "total_records": total_count,
                "etl_duration_seconds": etl_result[3] or 0,
                "status": "active"
            }
        else:
            return {
                "last_sync": None,
                "records_synced": 0,
                "total_records": total_count,
                "etl_duration_seconds": 0,
                "status": "no_sync_found"
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting ETL status: {str(e)}")