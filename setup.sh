#!/bin/bash

echo "🚀 Démarrage du système d'opérateurs..."

# Vérifier que Docker est en cours d'exécution
if ! docker info > /dev/null 2>&1; then
    echo "Docker n'est pas en cours d'exécution. Veuillez démarrer Docker."
    exit 1
fi

# Arrêter les conteneurs existants
echo "Arrêt des conteneurs existants..."
docker-compose down -v

# Reconstruire les images
echo "Reconstruction des images Docker..."
docker-compose build

# Démarrer les services
echo "Démarrage des services..."
docker-compose up -d postgres

# Attendre que PostgreSQL soit prêt
echo "Attente de PostgreSQL..."
sleep 10

# Démarrer les autres services
docker-compose up -d

echo "Attente que tous les services soient prêts..."
sleep 30

echo "Population de la base de données avec des données d'exemple..."
# Utiliser Docker pour peupler la base de données
echo "🐳 Exécution du script de population via Docker..."
docker run --rm \
    --network new_operator_default \
    -v "$(pwd)":/app \
    -w /app \
    -e PGHOST=postgres \
    -e PGPORT=5432 \
    -e PGUSER=myuser \
    -e PGPASSWORD=mypassword \
    -e PGDATABASE=metrics \
    python:3.11-slim \
    bash -c "pip install psycopg2-binary python-dotenv > /dev/null 2>&1 && python populate_operators.py"

echo "Synchronisation des données vers DuckDB..."
# Exécuter l'ETL directement dans le conteneur Airflow
docker-compose exec -T airflow-webserver python /opt/airflow/etl/etl_operators.py

echo "Système démarré avec succès !"
echo ""
echo "🌐 Services disponibles :"
echo "  - Airflow: http://localhost:8080 (admin/admin)"
echo "  - API FastAPI: http://localhost:8000"
echo "  - Frontend Next.js: http://localhost:3000"
echo "  - Documentation API: http://localhost:8000/docs"
echo ""
echo "Pour voir les statistiques des opérateurs :"
echo "  curl http://localhost:8000/operators/stats"
echo ""
echo "Pour gérer les services :"
echo "  - Arrêter: docker-compose down"
echo "  - Voir les logs: docker-compose logs -f"
echo "  - Redémarrer: docker-compose restart"
