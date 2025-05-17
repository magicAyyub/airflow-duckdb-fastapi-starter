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

-- Create the airflow database
CREATE DATABASE airflow;