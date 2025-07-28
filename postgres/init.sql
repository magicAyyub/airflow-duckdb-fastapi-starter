-- Create the metrics database
CREATE DATABASE metrics;

-- Connect to metrics database
\connect metrics;

-- Create metrics table
CREATE TABLE IF NOT EXISTS metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    value INTEGER
);

-- Create operators table with the schema you provided
CREATE TABLE IF NOT EXISTS operators (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(255),
    birth_name VARCHAR(255),
    middle_name VARCHAR(255),
    last_name VARCHAR(255),
    sex VARCHAR(10),
    birth_date DATE,
    cogville VARCHAR(255),
    cogpays VARCHAR(255),
    birth_city VARCHAR(255),
    birth_country VARCHAR(255),
    email VARCHAR(255),
    created_date DATE,
    archived_date DATE,
    uuid VARCHAR(255),
    id_ccu VARCHAR(255),
    subscription_channel VARCHAR(255),
    verification_mode VARCHAR(255),
    verification_date DATE,
    user_status VARCHAR(255),
    two_fa_status VARCHAR(255),
    first_activation_date DATE,
    expiration_date DATE,
    telephone VARCHAR(255),
    indicatif VARCHAR(255),
    date_modif_tel DATE,
    date_modf_tel DATE,
    numero_pi VARCHAR(255),
    expiration DATE,
    emission DATE,
    type VARCHAR(255),
    user_uuid VARCHAR(255),
    identity_verification_mode VARCHAR(255),
    identity_verification_status VARCHAR(255),
    identity_verification_result VARCHAR(255),
    id_identity_verification_proof VARCHAR(255),
    identity_verification_date DATE,
    operateur VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create some sample data
INSERT INTO operators (
    first_name, last_name, email, user_status, two_fa_status, 
    operateur, subscription_channel, verification_mode
) VALUES 
    ('Jean', 'Dupont', 'jean.dupont@email.com', 'ACTIVE', 'ENABLED', 'Orange', 'ONLINE', 'SMS'),
    ('Marie', 'Martin', 'marie.martin@email.com', 'ACTIVE', 'DISABLED', 'SFR', 'STORE', 'EMAIL'),
    ('Pierre', 'Bernard', 'pierre.bernard@email.com', 'SUSPENDED', 'ENABLED', 'Bouygues', 'ONLINE', 'SMS'),
    ('Sophie', 'Dubois', 'sophie.dubois@email.com', 'ACTIVE', 'ENABLED', 'Free', 'PHONE', 'CALL'),
    ('Antoine', 'Robert', 'antoine.robert@email.com', 'INACTIVE', 'DISABLED', 'Orange', 'ONLINE', 'SMS');

-- Create the airflow database
CREATE DATABASE airflow;