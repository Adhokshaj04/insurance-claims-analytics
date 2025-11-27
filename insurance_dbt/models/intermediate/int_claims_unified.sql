{{
    config(
        materialized='view',
        schema='staging'
    )
}}

/*
    Intermediate model: Union all claims from all sources
    Creates a single view of all claims across customers
*/

WITH customer_a_claims AS (
    SELECT
        source_system,
        source_policy_number,
        source_claim_number,
        
        -- Policy info
        policy_effective_date,
        policy_state,
        policy_annual_premium,
        policy_deductible,
        coverage_limit_bi,
        umbrella_limit,
        months_as_customer,
        
        -- Insured info
        age,
        gender,
        marital_status,
        education_level,
        occupation,
        NULL::DECIMAL AS income_annual,
        NULL::DECIMAL AS home_value,
        zip_code::TEXT AS zip_code,
        NULL::TEXT AS urbanicity,
        
        -- Vehicle info
        vehicle_year,
        vehicle_make,
        vehicle_model,
        vehicle_age,
        NULL::TEXT AS vehicle_type,
        NULL::DECIMAL AS vehicle_value,
        NULL::BOOLEAN AS is_red_car,
        
        -- Claim info
        incident_date,
        incident_type,
        collision_type,
        incident_severity,
        incident_state,
        incident_city,
        incident_hour,
        vehicles_involved,
        bodily_injuries_count,
        witnesses_count,
        police_report_available,
        authorities_contacted,
        NULL::BOOLEAN AS attorney_involved,
        NULL::BOOLEAN AS seatbelt_used,
        property_damage,
        
        -- Claim amounts
        total_claim_amount::DECIMAL,
        injury_claim_amount::DECIMAL,
        property_claim_amount::DECIMAL,
        vehicle_claim_amount::DECIMAL,
        
        -- Prior claims
        NULL::INTEGER AS prior_claim_count,
        NULL::DECIMAL AS prior_claim_total_amount,
        NULL::INTEGER AS mvr_points,
        NULL::BOOLEAN AS license_revoked,
        
        -- Fraud
        fraud_reported,
        
        -- Metadata
        load_timestamp
        
    FROM {{ ref('stg_customer_a') }}
),

customer_b_claims AS (
    SELECT
        source_system,
        NULL::TEXT AS source_policy_number,
        source_claim_number,
        
        -- Policy info
        NULL::DATE AS policy_effective_date,
        NULL::TEXT AS policy_state,
        NULL::DECIMAL AS policy_annual_premium,
        NULL::DECIMAL AS policy_deductible,
        NULL::DECIMAL AS coverage_limit_bi,
        NULL::DECIMAL AS umbrella_limit,
        NULL::INTEGER AS months_as_customer,
        
        -- Insured info
        age,
        gender,
        marital_status,
        NULL::TEXT AS education_level,
        NULL::TEXT AS occupation,
        NULL::DECIMAL AS income_annual,
        NULL::DECIMAL AS home_value,
        NULL::TEXT AS zip_code,
        NULL::TEXT AS urbanicity,
        
        -- Vehicle info
        NULL::INTEGER AS vehicle_year,
        NULL::TEXT AS vehicle_make,
        NULL::TEXT AS vehicle_model,
        NULL::INTEGER AS vehicle_age,
        NULL::TEXT AS vehicle_type,
        NULL::DECIMAL AS vehicle_value,
        NULL::BOOLEAN AS is_red_car,
        
        -- Claim info
        NULL::DATE AS incident_date,
        'Bodily Injury'::TEXT AS incident_type,
        NULL::TEXT AS collision_type,
        NULL::TEXT AS incident_severity,
        NULL::TEXT AS incident_state,
        NULL::TEXT AS incident_city,
        NULL::INTEGER AS incident_hour,
        NULL::INTEGER AS vehicles_involved,
        NULL::INTEGER AS bodily_injuries_count,
        NULL::INTEGER AS witnesses_count,
        NULL::BOOLEAN AS police_report_available,
        NULL::TEXT AS authorities_contacted,
        attorney_involved,
        seatbelt_used,
        NULL::BOOLEAN AS property_damage,
        
        -- Claim amounts
        total_claim_amount,
        injury_claim_amount,
        NULL::DECIMAL AS property_claim_amount,
        NULL::DECIMAL AS vehicle_claim_amount,
        
        -- Prior claims
        NULL::INTEGER AS prior_claim_count,
        NULL::DECIMAL AS prior_claim_total_amount,
        NULL::INTEGER AS mvr_points,
        NULL::BOOLEAN AS license_revoked,
        
        -- Fraud
        NULL::BOOLEAN AS fraud_reported,
        
        -- Metadata
        load_timestamp
        
    FROM {{ ref('stg_customer_b') }}
),

customer_c_claims AS (
    SELECT
        source_system,
        source_policy_number,
        source_policy_number AS source_claim_number,
        
        -- Policy info
        NULL::DATE AS policy_effective_date,
        NULL::TEXT AS policy_state,
        NULL::DECIMAL AS policy_annual_premium,
        NULL::DECIMAL AS policy_deductible,
        NULL::DECIMAL AS coverage_limit_bi,
        NULL::DECIMAL AS umbrella_limit,
        months_as_customer,
        
        -- Insured info
        age,
        gender,
        marital_status,
        education AS education_level,
        occupation,
        income_annual,
        home_value,
        NULL::TEXT AS zip_code,
        urbanicity,
        
        -- Vehicle info
        NULL::INTEGER AS vehicle_year,
        NULL::TEXT AS vehicle_make,
        NULL::TEXT AS vehicle_model,
        vehicle_age,
        vehicle_type,
        vehicle_value,
        is_red_car,
        
        -- Claim info
        NULL::DATE AS incident_date,
        NULL::TEXT AS incident_type,
        NULL::TEXT AS collision_type,
        NULL::TEXT AS incident_severity,
        NULL::TEXT AS incident_state,
        NULL::TEXT AS incident_city,
        NULL::INTEGER AS incident_hour,
        NULL::INTEGER AS vehicles_involved,
        NULL::INTEGER AS bodily_injuries_count,
        NULL::INTEGER AS witnesses_count,
        NULL::BOOLEAN AS police_report_available,
        NULL::TEXT AS authorities_contacted,
        NULL::BOOLEAN AS attorney_involved,
        NULL::BOOLEAN AS seatbelt_used,
        NULL::BOOLEAN AS property_damage,
        
        -- Claim amounts
        claim_amount AS total_claim_amount,
        NULL::DECIMAL AS injury_claim_amount,
        NULL::DECIMAL AS property_claim_amount,
        NULL::DECIMAL AS vehicle_claim_amount,
        
        -- Prior claims
        prior_claim_count,
        prior_claim_total_amount,
        mvr_points,
        license_revoked,
        
        -- Fraud
        NULL::BOOLEAN AS fraud_reported,
        
        -- Metadata
        load_timestamp
        
    FROM {{ ref('stg_customer_c') }}
    WHERE has_claim = TRUE
)

SELECT * FROM customer_a_claims
UNION ALL
SELECT * FROM customer_b_claims
UNION ALL
SELECT * FROM customer_c_claims
