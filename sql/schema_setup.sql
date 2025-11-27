-- ============================================================================
-- GUIDEWIRE INSURANCE DATA ANALYTICS PROJECT
-- Database Schema Setup Script
-- ============================================================================
-- Purpose: Create unified schema for harmonizing 3 insurance data sources
-- Author: [Your Name]
-- Date: November 2025
-- ============================================================================

-- Drop existing schema if it exists (for clean rebuild)
DROP SCHEMA IF EXISTS insurance_raw CASCADE;
DROP SCHEMA IF EXISTS insurance_staging CASCADE;
DROP SCHEMA IF EXISTS insurance_analytics CASCADE;

-- Create schemas for different layers
CREATE SCHEMA insurance_raw;      -- Raw data from sources
CREATE SCHEMA insurance_staging;  -- Cleaned and standardized data
CREATE SCHEMA insurance_analytics; -- Analytics-ready marts

-- ============================================================================
-- RAW LAYER - Store data as-is from each source
-- ============================================================================

-- Raw table for Dataset 1: insurance_claims.csv (Customer A)
CREATE TABLE insurance_raw.customer_a_claims (
    months_as_customer INTEGER,
    age INTEGER,
    policy_number BIGINT,
    policy_bind_date VARCHAR(20),
    policy_state VARCHAR(10),
    policy_csl VARCHAR(20),
    policy_deductable INTEGER,
    policy_annual_premium DECIMAL(10,2),
    umbrella_limit BIGINT,
    insured_zip INTEGER,
    insured_sex VARCHAR(10),
    insured_education_level VARCHAR(50),
    insured_occupation VARCHAR(100),
    insured_hobbies VARCHAR(100),
    insured_relationship VARCHAR(50),
    capital_gains INTEGER,
    capital_loss INTEGER,
    incident_date VARCHAR(20),
    incident_type VARCHAR(50),
    collision_type VARCHAR(50),
    incident_severity VARCHAR(50),
    authorities_contacted VARCHAR(50),
    incident_state VARCHAR(10),
    incident_city VARCHAR(100),
    incident_location VARCHAR(200),
    incident_hour_of_the_day INTEGER,
    number_of_vehicles_involved INTEGER,
    property_damage VARCHAR(10),
    bodily_injuries INTEGER,
    witnesses INTEGER,
    police_report_available VARCHAR(10),
    total_claim_amount INTEGER,
    injury_claim INTEGER,
    property_claim INTEGER,
    vehicle_claim INTEGER,
    auto_make VARCHAR(50),
    auto_model VARCHAR(50),
    auto_year INTEGER,
    fraud_reported VARCHAR(10),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(100) DEFAULT 'insurance_claims.csv'
);

-- Raw table for Dataset 2: AutoBi.csv (Customer B)
CREATE TABLE insurance_raw.customer_b_claims (
    index_id INTEGER,
    case_number INTEGER,
    attorney INTEGER,
    claimant_sex DECIMAL(5,2),
    marital_status DECIMAL(5,2),
    claimant_insured DECIMAL(5,2),
    seatbelt DECIMAL(5,2),
    claimant_age DECIMAL(5,2),
    loss_amount DECIMAL(10,3),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(100) DEFAULT 'AutoBi.csv'
);

-- Raw table for Dataset 3: car_insurance_claim.csv (Customer C)
CREATE TABLE insurance_raw.customer_c_policies (
    record_id BIGINT,
    kids_driving INTEGER,
    birth_date VARCHAR(20),
    age DECIMAL(5,2),
    home_kids INTEGER,
    years_on_job DECIMAL(5,2),
    income VARCHAR(20),
    parent1 VARCHAR(10),
    home_value VARCHAR(20),
    marital_status VARCHAR(20),
    gender VARCHAR(10),
    education VARCHAR(50),
    occupation VARCHAR(100),
    travel_time INTEGER,
    car_use VARCHAR(20),
    bluebook_value VARCHAR(20),
    time_in_force INTEGER,
    car_type VARCHAR(50),
    red_car VARCHAR(10),
    old_claim VARCHAR(20),
    claim_frequency INTEGER,
    license_revoked VARCHAR(10),
    mvr_points INTEGER,
    claim_amount VARCHAR(20),
    car_age DECIMAL(5,2),
    claim_flag INTEGER,
    urbanicity VARCHAR(50),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(100) DEFAULT 'car_insurance_claim.csv'
);

-- ============================================================================
-- STAGING LAYER - Cleaned and standardized data
-- ============================================================================
-- Note: DBT will create these tables with transformations

-- ============================================================================
-- ANALYTICS LAYER - Unified schema for analytics
-- ============================================================================

-- 1. Unified Policies Table
CREATE TABLE insurance_analytics.policies (
    policy_id VARCHAR(50) PRIMARY KEY,
    source_system VARCHAR(20) NOT NULL,
    source_policy_number VARCHAR(50),
    policy_effective_date DATE,
    policy_expiration_date DATE,
    policy_state VARCHAR(2),
    policy_annual_premium DECIMAL(10,2),
    policy_deductible DECIMAL(10,2),
    coverage_limit_bi DECIMAL(10,2),
    coverage_limit_pd DECIMAL(10,2),
    umbrella_limit DECIMAL(12,2),
    policy_status VARCHAR(20),
    months_as_customer INTEGER,
    time_in_force INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_quality_score INTEGER,
    CONSTRAINT chk_source CHECK (source_system IN ('customer_a', 'customer_b', 'customer_c')),
    CONSTRAINT chk_quality CHECK (data_quality_score BETWEEN 0 AND 100)
);

-- 2. Unified Insureds Table
CREATE TABLE insurance_analytics.insureds (
    insured_id VARCHAR(50) PRIMARY KEY,
    policy_id VARCHAR(50) REFERENCES insurance_analytics.policies(policy_id),
    source_system VARCHAR(20) NOT NULL,
    age INTEGER,
    birth_date DATE,
    gender VARCHAR(10),
    marital_status VARCHAR(20),
    education_level VARCHAR(50),
    occupation VARCHAR(100),
    income_annual DECIMAL(12,2),
    home_value DECIMAL(12,2),
    home_kids INTEGER,
    years_on_job INTEGER,
    relationship_to_policy VARCHAR(50),
    hobbies VARCHAR(100),
    travel_time_minutes INTEGER,
    zip_code VARCHAR(10),
    urbanicity VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_age CHECK (age BETWEEN 16 AND 100),
    CONSTRAINT chk_gender CHECK (gender IN ('M', 'F', 'Unknown'))
);

-- 3. Unified Vehicles Table
CREATE TABLE insurance_analytics.vehicles (
    vehicle_id VARCHAR(50) PRIMARY KEY,
    policy_id VARCHAR(50) REFERENCES insurance_analytics.policies(policy_id),
    source_system VARCHAR(20) NOT NULL,
    vehicle_year INTEGER,
    vehicle_make VARCHAR(50),
    vehicle_model VARCHAR(50),
    vehicle_type VARCHAR(50),
    vehicle_value DECIMAL(10,2),
    vehicle_age INTEGER,
    is_red_car BOOLEAN,
    car_use VARCHAR(20),
    kids_driving INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_vehicle_year CHECK (vehicle_year BETWEEN 1985 AND 2026)
);

-- 4. Unified Claims Table
CREATE TABLE insurance_analytics.claims (
    claim_id VARCHAR(50) PRIMARY KEY,
    policy_id VARCHAR(50) REFERENCES insurance_analytics.policies(policy_id),
    vehicle_id VARCHAR(50) REFERENCES insurance_analytics.vehicles(vehicle_id),
    source_system VARCHAR(20) NOT NULL,
    source_claim_number VARCHAR(50),
    incident_date DATE,
    report_date DATE,
    claim_status VARCHAR(20),
    incident_type VARCHAR(50),
    collision_type VARCHAR(50),
    incident_severity VARCHAR(20),
    incident_state VARCHAR(2),
    incident_city VARCHAR(100),
    incident_location VARCHAR(200),
    incident_hour INTEGER,
    vehicles_involved INTEGER,
    bodily_injuries_count INTEGER,
    witnesses_count INTEGER,
    police_report_available BOOLEAN,
    authorities_contacted VARCHAR(50),
    attorney_involved BOOLEAN,
    seatbelt_used BOOLEAN,
    property_damage BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_incident_hour CHECK (incident_hour BETWEEN 0 AND 23)
);

-- 5. Unified Claim Amounts Table
CREATE TABLE insurance_analytics.claim_amounts (
    claim_amount_id VARCHAR(50) PRIMARY KEY,
    claim_id VARCHAR(50) REFERENCES insurance_analytics.claims(claim_id),
    source_system VARCHAR(20) NOT NULL,
    total_claim_amount DECIMAL(12,2),
    injury_claim_amount DECIMAL(12,2),
    property_claim_amount DECIMAL(12,2),
    vehicle_claim_amount DECIMAL(12,2),
    claim_paid_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_amounts_positive CHECK (
        total_claim_amount >= 0 AND
        COALESCE(injury_claim_amount, 0) >= 0 AND
        COALESCE(property_claim_amount, 0) >= 0 AND
        COALESCE(vehicle_claim_amount, 0) >= 0
    )
);

-- 6. Claim History Table (Prior Claims)
CREATE TABLE insurance_analytics.claim_history (
    history_id VARCHAR(50) PRIMARY KEY,
    policy_id VARCHAR(50) REFERENCES insurance_analytics.policies(policy_id),
    insured_id VARCHAR(50) REFERENCES insurance_analytics.insureds(insured_id),
    source_system VARCHAR(20) NOT NULL,
    prior_claim_count INTEGER DEFAULT 0,
    prior_claim_total_amount DECIMAL(12,2),
    most_recent_claim_date DATE,
    mvr_points INTEGER,
    license_revoked BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 7. Fraud Indicators Table
CREATE TABLE insurance_analytics.fraud_indicators (
    fraud_id VARCHAR(50) PRIMARY KEY,
    claim_id VARCHAR(50) REFERENCES insurance_analytics.claims(claim_id),
    source_system VARCHAR(20) NOT NULL,
    fraud_reported BOOLEAN,
    fraud_score DECIMAL(5,2),
    risk_factors TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_fraud_score CHECK (fraud_score BETWEEN 0 AND 100)
);

-- 8. Data Quality Log Table
CREATE TABLE insurance_analytics.data_quality_log (
    log_id SERIAL PRIMARY KEY,
    source_system VARCHAR(20),
    table_name VARCHAR(50),
    record_id VARCHAR(50),
    issue_type VARCHAR(100),
    issue_description TEXT,
    severity VARCHAR(20),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP,
    resolved_by VARCHAR(100),
    CONSTRAINT chk_severity CHECK (severity IN ('Critical', 'High', 'Medium', 'Low'))
);

-- ============================================================================
-- INDEXES for Performance
-- ============================================================================

-- Policies indexes
CREATE INDEX idx_policies_source ON insurance_analytics.policies(source_system);
CREATE INDEX idx_policies_state ON insurance_analytics.policies(policy_state);
CREATE INDEX idx_policies_effective_date ON insurance_analytics.policies(policy_effective_date);

-- Insureds indexes
CREATE INDEX idx_insureds_policy ON insurance_analytics.insureds(policy_id);
CREATE INDEX idx_insureds_age ON insurance_analytics.insureds(age);
CREATE INDEX idx_insureds_source ON insurance_analytics.insureds(source_system);

-- Vehicles indexes
CREATE INDEX idx_vehicles_policy ON insurance_analytics.vehicles(policy_id);
CREATE INDEX idx_vehicles_make_model ON insurance_analytics.vehicles(vehicle_make, vehicle_model);

-- Claims indexes
CREATE INDEX idx_claims_policy ON insurance_analytics.claims(policy_id);
CREATE INDEX idx_claims_incident_date ON insurance_analytics.claims(incident_date);
CREATE INDEX idx_claims_source ON insurance_analytics.claims(source_system);
CREATE INDEX idx_claims_status ON insurance_analytics.claims(claim_status);

-- Claim amounts indexes
CREATE INDEX idx_amounts_claim ON insurance_analytics.claim_amounts(claim_id);
CREATE INDEX idx_amounts_total ON insurance_analytics.claim_amounts(total_claim_amount);

-- Data quality log indexes
CREATE INDEX idx_dq_log_source ON insurance_analytics.data_quality_log(source_system);
CREATE INDEX idx_dq_log_severity ON insurance_analytics.data_quality_log(severity);
CREATE INDEX idx_dq_log_resolved ON insurance_analytics.data_quality_log(resolved);

-- ============================================================================
-- VIEWS for Common Analytics Queries
-- ============================================================================

-- View: Complete claim information with policy and insured details
CREATE OR REPLACE VIEW insurance_analytics.vw_claims_complete AS
SELECT 
    c.claim_id,
    c.source_system,
    c.incident_date,
    c.incident_type,
    c.incident_severity,
    c.incident_state,
    p.policy_id,
    p.policy_state,
    p.policy_annual_premium,
    i.insured_id,
    i.age,
    i.gender,
    i.occupation,
    v.vehicle_make,
    v.vehicle_model,
    v.vehicle_year,
    ca.total_claim_amount,
    ca.injury_claim_amount,
    ca.property_claim_amount,
    ca.vehicle_claim_amount,
    f.fraud_reported
FROM insurance_analytics.claims c
LEFT JOIN insurance_analytics.policies p ON c.policy_id = p.policy_id
LEFT JOIN insurance_analytics.insureds i ON p.policy_id = i.policy_id
LEFT JOIN insurance_analytics.vehicles v ON c.vehicle_id = v.vehicle_id
LEFT JOIN insurance_analytics.claim_amounts ca ON c.claim_id = ca.claim_id
LEFT JOIN insurance_analytics.fraud_indicators f ON c.claim_id = f.claim_id;

-- View: Data quality metrics by source
CREATE OR REPLACE VIEW insurance_analytics.vw_data_quality_summary AS
SELECT 
    source_system,
    COUNT(*) as total_policies,
    AVG(data_quality_score) as avg_quality_score,
    MIN(data_quality_score) as min_quality_score,
    MAX(data_quality_score) as max_quality_score
FROM insurance_analytics.policies
GROUP BY source_system;

-- View: Claims summary by source system
CREATE OR REPLACE VIEW insurance_analytics.vw_claims_by_source AS
SELECT 
    source_system,
    COUNT(*) as total_claims,
    SUM(total_claim_amount) as total_claim_dollars,
    AVG(total_claim_amount) as avg_claim_amount,
    MIN(incident_date) as earliest_claim,
    MAX(incident_date) as latest_claim
FROM insurance_analytics.claims c
JOIN insurance_analytics.claim_amounts ca ON c.claim_id = ca.claim_id
GROUP BY source_system;

-- ============================================================================
-- COMMENTS for Documentation
-- ============================================================================

COMMENT ON SCHEMA insurance_raw IS 'Raw data layer - stores unmodified data from source systems';
COMMENT ON SCHEMA insurance_staging IS 'Staging layer - cleaned and standardized data (managed by DBT)';
COMMENT ON SCHEMA insurance_analytics IS 'Analytics layer - unified schema for reporting and analysis';

COMMENT ON TABLE insurance_analytics.policies IS 'Unified policy information from all sources';
COMMENT ON TABLE insurance_analytics.insureds IS 'Unified insured/policyholder information';
COMMENT ON TABLE insurance_analytics.vehicles IS 'Unified vehicle information';
COMMENT ON TABLE insurance_analytics.claims IS 'Unified claims incidents';
COMMENT ON TABLE insurance_analytics.claim_amounts IS 'Financial claim amounts';
COMMENT ON TABLE insurance_analytics.fraud_indicators IS 'Fraud detection indicators';
COMMENT ON TABLE insurance_analytics.data_quality_log IS 'Log of data quality issues and resolutions';

-- ============================================================================
-- GRANT PERMISSIONS (adjust based on your user setup)
-- ============================================================================

-- Grant read-only access to analytics schema for reporting users
-- GRANT USAGE ON SCHEMA insurance_analytics TO reporting_user;
-- GRANT SELECT ON ALL TABLES IN SCHEMA insurance_analytics TO reporting_user;

-- ============================================================================
-- COMPLETION MESSAGE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '====================================================================';
    RAISE NOTICE 'Insurance Data Analytics Database Schema Created Successfully!';
    RAISE NOTICE '====================================================================';
    RAISE NOTICE 'Schemas Created:';
    RAISE NOTICE '  - insurance_raw (raw data storage)';
    RAISE NOTICE '  - insurance_staging (DBT transformations)';
    RAISE NOTICE '  - insurance_analytics (analytics tables)';
    RAISE NOTICE '';
    RAISE NOTICE 'Tables Created: 11';
    RAISE NOTICE 'Views Created: 3';
    RAISE NOTICE 'Indexes Created: 15';
    RAISE NOTICE '====================================================================';
END $$;
