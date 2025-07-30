#!/usr/bin/env python3
"""
Script pour peupler la base de données PostgreSQL avec des données d'opérateurs télécom.
"""

import psycopg2
from datetime import datetime, timedelta
import random
import os
import sys
import uuid

# Configuration de connexion
DB_CONFIG = {
    'host': os.getenv('PGHOST', 'localhost'),
    'port': os.getenv('PGPORT', '5432'),
    'database': os.getenv('PGDATABASE', 'metrics'),
    'user': os.getenv('PGUSER', 'myuser'),
    'password': os.getenv('PGPASSWORD', 'mypassword')
}

def generate_random_operators(count=55):
    """Génère des données d'opérateurs télécom aléatoires."""
    
    # Données réalistes pour les opérateurs télécom
    first_names = ['Jean', 'Marie', 'Pierre', 'Sophie', 'Paul', 'Julie', 'Michel', 'Anne', 'Philippe', 'Catherine',
                   'Alain', 'Sylvie', 'Patrick', 'Isabelle', 'Nicolas', 'Françoise', 'Daniel', 'Monique', 'Claude', 'Nathalie']
    
    last_names = ['Dupont', 'Martin', 'Bernard', 'Moreau', 'Petit', 'Durand', 'Leroy', 'Simon', 'Michel', 'Garcia',
                  'David', 'Bertrand', 'Roux', 'Vincent', 'Fournier', 'Morel', 'Girard', 'Andre', 'Lefebvre', 'Mercier']
    
    operateurs = ['Orange', 'SFR', 'Bouygues', 'Free', 'La Poste Mobile']
    user_statuses = ['ACTIVE', 'INACTIVE', 'SUSPENDED', 'PENDING']
    two_fa_statuses = ['ENABLED', 'DISABLED', 'PENDING']
    subscription_channels = ['ONLINE', 'STORE', 'PHONE', 'PARTNER']
    verification_modes = ['SMS', 'EMAIL', 'CALL', 'NONE']
    
    cities = ['Paris', 'Lyon', 'Marseille', 'Toulouse', 'Nice', 'Nantes', 'Strasbourg', 'Montpellier', 'Bordeaux', 'Lille']
    countries = ['France', 'Belgique', 'Suisse', 'Luxembourg', 'Monaco']
    
    operators = []
    used_emails = set()
    used_uuids = set()
    
    for i in range(count):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        
        # Email unique
        while True:
            email = f"{first_name.lower()}.{last_name.lower()}@email.com"
            if email not in used_emails:
                used_emails.add(email)
                break
            else:
                email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@email.com"
                if email not in used_emails:
                    used_emails.add(email)
                    break
        
        # UUID unique
        while True:
            user_uuid = str(uuid.uuid4())
            if user_uuid not in used_uuids:
                used_uuids.add(user_uuid)
                break
        
        # Dates aléatoires
        created_date = datetime.now() - timedelta(days=random.randint(30, 1095))  # 1 mois à 3 ans
        birth_date = datetime.now() - timedelta(days=random.randint(6570, 25550))  # 18 à 70 ans
        
        verification_date = None
        if random.choice([True, False]):
            verification_date = created_date + timedelta(days=random.randint(1, 30))
        
        first_activation_date = None
        if random.choice([True, False]):
            first_activation_date = created_date + timedelta(days=random.randint(1, 7))
        
        expiration_date = None
        if random.choice([True, False]):
            expiration_date = datetime.now() + timedelta(days=random.randint(30, 365))
        
        operator = {
            'first_name': first_name,
            'birth_name': last_name if random.choice([True, False]) else None,
            'middle_name': random.choice(first_names) if random.choice([True, False]) else None,
            'last_name': last_name,
            'sex': random.choice(['M', 'F']),
            'birth_date': birth_date.strftime('%Y-%m-%d'),
            'cogville': random.choice(cities) if random.choice([True, False]) else None,
            'cogpays': random.choice(countries) if random.choice([True, False]) else None,
            'birth_city': random.choice(cities),
            'birth_country': random.choice(countries),
            'email': email,
            'created_date': created_date.strftime('%Y-%m-%d'),
            'archived_date': None,  # Pas d'archivage pour les nouveaux comptes
            'uuid': user_uuid,
            'id_ccu': f"CCU{random.randint(100000, 999999)}",
            'subscription_channel': random.choice(subscription_channels),
            'verification_mode': random.choice(verification_modes),
            'verification_date': verification_date.strftime('%Y-%m-%d') if verification_date else None,
            'user_status': random.choice(user_statuses),
            'two_fa_status': random.choice(two_fa_statuses),
            'first_activation_date': first_activation_date.strftime('%Y-%m-%d') if first_activation_date else None,
            'expiration_date': expiration_date.strftime('%Y-%m-%d') if expiration_date else None,
            'telephone': f"+33{random.randint(100000000, 999999999)}",
            'indicatif': '+33',
            'date_modif_tel': None,
            'date_modf_tel': None,
            'numero_pi': f"PI{random.randint(10000000, 99999999)}" if random.choice([True, False]) else None,
            'expiration': expiration_date.strftime('%Y-%m-%d') if expiration_date and random.choice([True, False]) else None,
            'emission': created_date.strftime('%Y-%m-%d') if random.choice([True, False]) else None,
            'type': random.choice(['PREPAID', 'POSTPAID', 'CORPORATE']),
            'user_uuid': user_uuid,
            'identity_verification_mode': random.choice(['AUTOMATIC', 'MANUAL', 'NONE']),
            'identity_verification_status': random.choice(['VERIFIED', 'PENDING', 'FAILED', 'NOT_STARTED']),
            'identity_verification_result': random.choice(['SUCCESS', 'FAILURE', 'PENDING']) if random.choice([True, False]) else None,
            'id_identity_verification_proof': f"PROOF{random.randint(100000, 999999)}" if random.choice([True, False]) else None,
            'identity_verification_date': verification_date.strftime('%Y-%m-%d') if verification_date and random.choice([True, False]) else None,
            'operateur': random.choice(operateurs)
        }
        operators.append(operator)
    
    return operators

def populate_database():
    """Peuple la base de données avec les données d'opérateurs télécom."""
    try:
        # Connexion à la base de données
        print(f"Connexion à PostgreSQL sur {DB_CONFIG['host']}:{DB_CONFIG['port']}")
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✅ Connexion réussie !")
        
        # Générer les données d'opérateurs télécom
        print("Génération des données d'opérateurs télécom...")
        operators = generate_random_operators(55)
        
        # Insérer les données
        insert_query = """
        INSERT INTO operators (
            first_name, birth_name, middle_name, last_name, sex, birth_date,
            cogville, cogpays, birth_city, birth_country, email, created_date,
            archived_date, uuid, id_ccu, subscription_channel, verification_mode,
            verification_date, user_status, two_fa_status, first_activation_date,
            expiration_date, telephone, indicatif, date_modif_tel, date_modf_tel,
            numero_pi, expiration, emission, type, user_uuid,
            identity_verification_mode, identity_verification_status,
            identity_verification_result, id_identity_verification_proof,
            identity_verification_date, operateur
        ) VALUES (
            %(first_name)s, %(birth_name)s, %(middle_name)s, %(last_name)s,
            %(sex)s, %(birth_date)s, %(cogville)s, %(cogpays)s, %(birth_city)s,
            %(birth_country)s, %(email)s, %(created_date)s, %(archived_date)s,
            %(uuid)s, %(id_ccu)s, %(subscription_channel)s, %(verification_mode)s,
            %(verification_date)s, %(user_status)s, %(two_fa_status)s,
            %(first_activation_date)s, %(expiration_date)s, %(telephone)s,
            %(indicatif)s, %(date_modif_tel)s, %(date_modf_tel)s, %(numero_pi)s,
            %(expiration)s, %(emission)s, %(type)s, %(user_uuid)s,
            %(identity_verification_mode)s, %(identity_verification_status)s,
            %(identity_verification_result)s, %(id_identity_verification_proof)s,
            %(identity_verification_date)s, %(operateur)s
        );
        """
        
        print(f"Insertion de {len(operators)} opérateurs télécom...")
        cursor.executemany(insert_query, operators)
        
        # Valider les changements
        conn.commit()
        
        # Vérifier l'insertion
        cursor.execute("SELECT COUNT(*) FROM operators;")
        final_count = cursor.fetchone()[0]
        
        print(f"✅ {final_count} opérateurs télécom insérés avec succès !")
        
        # Afficher quelques statistiques
        cursor.execute("""
            SELECT 
                operateur,
                COUNT(*) as count,
                COUNT(CASE WHEN user_status = 'ACTIVE' THEN 1 END) as active_count
            FROM operators 
            GROUP BY operateur
            ORDER BY count DESC;
        """)
        
        print("\nRépartition par opérateur :")
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]} clients (dont {row[2]} actifs)")
        
        # Statistiques sur les statuts 2FA
        cursor.execute("""
            SELECT 
                two_fa_status,
                COUNT(*) as count
            FROM operators 
            GROUP BY two_fa_status
            ORDER BY count DESC;
        """)
        
        print("\nRépartition 2FA :")
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]} clients")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"❌ Erreur PostgreSQL : {e}")
    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    print("🚀 Population de la base de données avec des opérateurs télécom...")
    populate_database()
