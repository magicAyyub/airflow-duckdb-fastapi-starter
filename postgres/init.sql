-- Create the metrics database
CREATE DATABASE metrics;

-- Connect to metrics database
\connect metrics;

-- Create user_data table with actual structure
CREATE TABLE IF NOT EXISTS user_data (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(255),
    birth_name VARCHAR(255),
    middle_name VARCHAR(255),
    last_name VARCHAR(255),
    sex CHAR(1),
    birth_date DATE,
    cogville VARCHAR(10),
    cogpays VARCHAR(10),
    birth_city VARCHAR(255),
    birth_country VARCHAR(255),
    email VARCHAR(255),
    created_date TIMESTAMPTZ DEFAULT NOW(),
    uuid VARCHAR(255) UNIQUE,
    id_ccu VARCHAR(255),
    subscription_channel VARCHAR(255),
    verification_mode VARCHAR(255),
    verification_date TIMESTAMPTZ,
    user_status VARCHAR(50),
    tfa_status VARCHAR(50),
    first_activation_date TIMESTAMPTZ,
    expiration_date TIMESTAMPTZ,
    telephone VARCHAR(20),
    indicatif VARCHAR(10),
    date_modif_tel TIMESTAMPTZ,
    numero_pi VARCHAR(255),
    expiration_doc DATE,
    emission_doc DATE,
    type_doc VARCHAR(50),
    user_uuid VARCHAR(255),
    identity_verification_mode VARCHAR(255),
    identity_verification_status VARCHAR(50),
    identity_verification_result VARCHAR(50),
    id_identity_verification_proof VARCHAR(255),
    identity_verification_date TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(uuid)
);

-- Create operator mapping table (MAJNUM equivalent)
CREATE TABLE IF NOT EXISTS operator_mapping (
    id SERIAL PRIMARY KEY,
    tranche_debut VARCHAR(20),  -- Increased from 10 to 20
    tranche_fin VARCHAR(20),    -- Increased from 10 to 20
    date_attribution DATE,
    ezabpqm VARCHAR(20),        -- Increased from 10 to 20 for longer prefixes
    mnemo VARCHAR(50),          -- This will become "Operateur"
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_user_data_updated_at BEFORE UPDATE ON user_data 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_operator_mapping_updated_at BEFORE UPDATE ON operator_mapping 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create the airflow database
CREATE DATABASE airflow;