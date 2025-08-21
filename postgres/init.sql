-- Create the metrics database
CREATE DATABASE metrics;

-- Connect to metrics database
\connect metrics;

-- Create user_data table with actual structure matching the old CSV format
CREATE TABLE IF NOT EXISTS user_data (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(255),        -- FIRST_NAME
    birth_name VARCHAR(255),        -- BIRTH_NAME  
    middle_name VARCHAR(255),       -- MIDDLE_NAME
    last_name VARCHAR(255),         -- LAST_NAME
    sex CHAR(1),                    -- SEX
    birth_date DATE,                -- BIRTH_DATE
    cogville VARCHAR(10),           -- COGVILLE
    cogpays VARCHAR(10),            -- COGPAYS
    birth_city VARCHAR(255),        -- BIRTH_CITY
    birth_country VARCHAR(255),     -- BIRTH_COUNTRY
    email VARCHAR(255),             -- EMAIL
    created_date TIMESTAMPTZ DEFAULT NOW(),  -- CREATED_DATE
    uuid VARCHAR(255) UNIQUE,       -- UUID
    id_ccu VARCHAR(255),           -- ID_CCU
    subscription_channel VARCHAR(255), -- SUBSCRIPTION_CHANNEL
    verification_mode VARCHAR(255), -- VERIFICATION_MODE
    verification_date TIMESTAMPTZ,  -- VERIFICATION_DATE
    user_status VARCHAR(50),        -- USER_STATUS
    tfa_status VARCHAR(50),         -- 2FA_STATUS (note: tfa_status in DB maps to "2FA_STATUS" in CSV)
    first_activation_date TIMESTAMPTZ, -- FIRST_ACTIVATION_DATE
    expiration_date TIMESTAMPTZ,    -- EXPIRATION_DATE
    telephone VARCHAR(20),          -- TELEPHONE
    indicatif VARCHAR(10),          -- INDICATIF
    date_modif_tel TIMESTAMPTZ,     -- DATE_MODIF_TEL
    numero_pi VARCHAR(255),         -- Numero Pi
    expiration_doc DATE,            -- EXPIRATION
    emission_doc DATE,              -- EMISSION
    type_doc VARCHAR(50),           -- TYPE
    user_uuid VARCHAR(255),         -- USER_UUID
    identity_verification_mode VARCHAR(255),     -- IDENTITY_VERIFICATION_MODE
    identity_verification_status VARCHAR(50),    -- IDENTITY_VERIFICATION_STATUS
    identity_verification_result VARCHAR(50),    -- IDENTITY_VERIFICATION_RESULT
    id_identity_verification_proof VARCHAR(255), -- ID_IDENTITY_VERIFICATION_PROOF
    identity_verification_date TIMESTAMPTZ,      -- IDENTITY_VERIFICATION_DATE
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