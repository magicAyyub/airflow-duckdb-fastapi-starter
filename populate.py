import os
import psycopg2
import time
import pandas as pd
from random import randint, choice, uniform
from datetime import datetime, timedelta
from faker import Faker
import uuid

fake = Faker('fr_FR')

# Database connection
conn = psycopg2.connect(
    dbname="metrics",
    user="myuser",
    password="mypassword",
    host="localhost",
    port="5432"
)

cursor = conn.cursor()

def load_majnum_data():
    """Load real MAJNUM.csv data into operator_mapping table"""
    print("Chargement des données MAJNUM.csv...")
    
    try:
        # Read the MAJNUM.csv file
        majnum_df = pd.read_csv('MAJNUM.csv', sep=';', encoding='latin-1')
        
        # Clear existing data
        cursor.execute("DELETE FROM operator_mapping;")
        
        # Insert real data
        inserted_count = 0
        for _, row in majnum_df.iterrows():
            try:
                # Clean and prepare data
                ezabpqm = str(row['EZABPQM']).strip()
                tranche_debut = str(row['Tranche_Debut']).strip()
                tranche_fin = str(row['Tranche_Fin']).strip()
                mnemo = str(row['Mnémo']).strip()
                date_attr = row['Date_Attribution']
                
                # Convert date format from DD/MM/YYYY to YYYY-MM-DD
                try:
                    if isinstance(date_attr, str) and '/' in date_attr:
                        day, month, year = date_attr.split('/')
                        date_attr = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    elif pd.isna(date_attr):
                        date_attr = '2017-01-01'  # Default date
                except:
                    date_attr = '2017-01-01'  # Default date if parsing fails
                
                cursor.execute("""
                    INSERT INTO operator_mapping (ezabpqm, tranche_debut, tranche_fin, mnemo, date_attribution)
                    VALUES (%s, %s, %s, %s, %s)
                """, (ezabpqm, tranche_debut, tranche_fin, mnemo, date_attr))
                inserted_count += 1
                
            except Exception as row_error:
                print(f"Erreur ligne {inserted_count + 1}: {row_error}")
                # Continue with next row
                continue
        
        conn.commit()
        print(f"{inserted_count}/{len(majnum_df)} entrées MAJNUM chargées avec succès")
        
        # Show operator distribution
        cursor.execute("""
            SELECT mnemo, COUNT(*) as count 
            FROM operator_mapping 
            GROUP BY mnemo 
            ORDER BY count DESC
        """)
        operators = cursor.fetchall()
        print("Répartition des opérateurs:")
        for op, count in operators[:10]:  # Top 10
            print(f"   {op}: {count} entrées")
            
        return True
        
    except FileNotFoundError:
        print("Fichier MAJNUM.csv non trouvé. Utilisation des données par défaut...")
        conn.rollback()  # Rollback any failed transaction
        return populate_default_operator_mapping()
    except Exception as e:
        print(f"Erreur lors du chargement MAJNUM: {e}")
        print("Utilisation des données par défaut...")
        conn.rollback()  # Rollback any failed transaction
        return populate_default_operator_mapping()

def populate_default_operator_mapping():
    """Fallback: populate with simplified operator data if MAJNUM.csv is not available"""
    print("Peuplement avec les données opérateur par défaut...")
    
    try:
        # Clear existing data
        cursor.execute("DELETE FROM operator_mapping;")
        
        # Simplified French mobile operator data
        default_operators = [
            # Orange mobile prefixes
            ('06', '0600000000', '0699999999', 'ORAN', '2007-01-01'),
            ('07', '0700000000', '0799999999', 'ORAN', '2009-01-01'),
            # SFR prefixes
            ('61', '0610000000', '0619999999', 'SFR0', '2007-01-01'),
            ('62', '0620000000', '0629999999', 'SFR0', '2007-01-01'),
            ('63', '0630000000', '0639999999', 'SFR0', '2007-01-01'),
            # Bouygues prefixes  
            ('65', '0650000000', '0659999999', 'BOUY', '2007-01-01'),
            ('66', '0660000000', '0669999999', 'BOUY', '2007-01-01'),
            ('67', '0670000000', '0679999999', 'BOUY', '2007-01-01'),
            # Free prefixes
            ('75', '0750000000', '0759999999', 'FRTE', '2012-01-01'),
            ('76', '0760000000', '0769999999', 'FRTE', '2012-01-01'),
            ('77', '0770000000', '0779999999', 'FRTE', '2012-01-01'),
        ]
        
        for ezabpqm, tranche_debut, tranche_fin, mnemo, date_attr in default_operators:
            cursor.execute("""
                INSERT INTO operator_mapping (ezabpqm, tranche_debut, tranche_fin, mnemo, date_attribution)
                VALUES (%s, %s, %s, %s, %s)
            """, (ezabpqm, tranche_debut, tranche_fin, mnemo, date_attr))
        
        conn.commit()
        print(f"{len(default_operators)} opérateurs par défaut ajoutés")
        return True
        
    except Exception as e:
        print(f"Erreur lors du peuplement par défaut: {e}")
        conn.rollback()
        return False

def get_realistic_french_mobile():
    """Generate a realistic French mobile number based on loaded operator data"""
    # Get available prefixes from database
    cursor.execute("SELECT DISTINCT ezabpqm FROM operator_mapping WHERE LENGTH(ezabpqm) = 2")
    prefixes = [row[0] for row in cursor.fetchall()]
    
    if not prefixes:
        # Fallback to default prefixes
        prefixes = ['06', '07', '61', '62', '63', '65', '66', '67', '75', '76', '77']
    
    prefix = choice(prefixes)
    suffix = f"{randint(10,99)}{randint(10,99)}{randint(10,99)}{randint(10,99)}"
    return f"33{prefix}{suffix}"

def generate_user_data():
    """Generate realistic user data"""
    sex = choice(['M', 'F'])
    first_name = fake.first_name_male() if sex == 'M' else fake.first_name_female()
    last_name = fake.last_name()
    birth_date = fake.date_of_birth(minimum_age=18, maximum_age=80)
    
    # Generate dates in logical order
    created_date = fake.date_time_between(start_date='-2y', end_date='now')
    verification_date = created_date + timedelta(minutes=randint(5, 60))
    first_activation_date = verification_date + timedelta(minutes=randint(1, 30))
    expiration_date = first_activation_date + timedelta(days=365*5)  # 5 years validity
    
    doc_emission = fake.date_between(start_date='-10y', end_date='-1y')
    doc_expiration = doc_emission + timedelta(days=365*10)  # 10 years validity
    
    return {
        'first_name': first_name,
        'birth_name': last_name,
        'middle_name': fake.first_name() if randint(0, 3) == 0 else None,
        'last_name': last_name,
        'sex': sex,
        'birth_date': birth_date,
        'cogville': f"{randint(10000, 99999)}",
        'cogpays': choice(['99100', '99216', '99352', '99127']),  # France and various countries
        'birth_city': fake.city(),
        'birth_country': choice(['FRANCE', 'ALGERIE', 'MAROC', 'TUNISIE', 'ITALIE', 'ESPAGNE']),
        'email': fake.email(),
        'created_date': created_date,
        'uuid': str(uuid.uuid4()),
        'id_ccu': str(randint(5000000000000, 7999999999999)),
        'subscription_channel': choice(['LIN_APP_Ios', 'LIN_APP_Android', 'LIN_WEB', 'LIN_BP']),
        'verification_mode': choice(['PVID', 'LRE', 'EERPOSTOFFICE', '']),
        'verification_date': verification_date,
        'user_status': choice(['verified', 'prospect']),
        'tfa_status': choice(['activated', 'null']),
        'first_activation_date': first_activation_date,
        'expiration_date': expiration_date,
        'telephone': get_realistic_french_mobile(),
        'indicatif': '+33',
        'date_modif_tel': created_date + timedelta(days=randint(0, 30)),
        'numero_pi': f"{''.join(fake.random_letters(length=3)).upper()}{randint(10,99)}{''.join(fake.random_letters(length=3)).upper()}{randint(0,9)}",
        'expiration_doc': doc_expiration,
        'emission_doc': doc_emission,
        'type_doc': choice(['ID_CARD', 'PASSPORT', 'RESIDENT_PERMIT']),
        'user_uuid': str(uuid.uuid4()),
        'identity_verification_mode': choice(['PVID', 'LRE', 'EERPOSTOFFICE']),
        'identity_verification_status': choice(['CLOSED', 'CREATED']),
        'identity_verification_result': choice(['VERIFIED', 'PENDING']),
        'id_identity_verification_proof': str(uuid.uuid4()),
        'identity_verification_date': verification_date
    }

def insert_user_batch(batch_size=10):
    """Insert a batch of users"""
    users = []
    for _ in range(batch_size):
        users.append(generate_user_data())
    
    insert_query = """
        INSERT INTO user_data (
            first_name, birth_name, middle_name, last_name, sex, birth_date,
            cogville, cogpays, birth_city, birth_country, email, created_date,
            uuid, id_ccu, subscription_channel, verification_mode, verification_date,
            user_status, tfa_status, first_activation_date, expiration_date,
            telephone, indicatif, date_modif_tel, numero_pi, expiration_doc,
            emission_doc, type_doc, user_uuid, identity_verification_mode,
            identity_verification_status, identity_verification_result,
            id_identity_verification_proof, identity_verification_date
        ) VALUES (
            %(first_name)s, %(birth_name)s, %(middle_name)s, %(last_name)s, %(sex)s, %(birth_date)s,
            %(cogville)s, %(cogpays)s, %(birth_city)s, %(birth_country)s, %(email)s, %(created_date)s,
            %(uuid)s, %(id_ccu)s, %(subscription_channel)s, %(verification_mode)s, %(verification_date)s,
            %(user_status)s, %(tfa_status)s, %(first_activation_date)s, %(expiration_date)s,
            %(telephone)s, %(indicatif)s, %(date_modif_tel)s, %(numero_pi)s, %(expiration_doc)s,
            %(emission_doc)s, %(type_doc)s, %(user_uuid)s, %(identity_verification_mode)s,
            %(identity_verification_status)s, %(identity_verification_result)s,
            %(id_identity_verification_proof)s, %(identity_verification_date)s
        ) ON CONFLICT (uuid) DO NOTHING
    """
    
    try:
        cursor.executemany(insert_query, users)
        conn.commit()
        return len(users)
    except Exception as e:
        print(f"Erreur lors de l'insertion: {e}")
        conn.rollback()
        return 0

print("🌱 Démarrage de l'injection des données utilisateur...")

# First, load MAJNUM data (with fallback to default)
load_majnum_data()

# Then populate user data
try:
    # Insert initial batch
    initial_batch = insert_user_batch(100)
    print(f"{initial_batch} utilisateurs initiaux créés")
    
    # Continuous insertion to simulate live system
    while True:
        new_users = insert_user_batch(randint(1, 5))
        if new_users > 0:
            print(f"✓ {new_users} nouveaux utilisateurs ajoutés à {datetime.now().strftime('%H:%M:%S')}")
        time.sleep(randint(10, 30))  # Random interval between 10-30 seconds
        
except KeyboardInterrupt:
    print("Arrêt manuel.")
finally:
    cursor.close()
    conn.close()