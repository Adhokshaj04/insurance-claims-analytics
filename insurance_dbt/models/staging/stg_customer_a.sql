{{
    config(
        materialized='view',
        schema='staging'
    )
}}

/*
    Staging model for Customer A (insurance_claims.csv)
    Cleans and standardizes the comprehensive claims data
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'customer_a_claims') }}
),

cleaned AS (
    SELECT
        -- Source tracking
        'customer_a' AS source_system,
        policy_number::TEXT AS source_policy_number,
        policy_number::TEXT AS source_claim_number,
        
        -- Policy information
        policy_bind_date::DATE AS policy_effective_date,
        policy_state,
        policy_annual_premium,
        policy_deductable AS policy_deductible,
        CASE 
            WHEN policy_csl = '100/300' THEN 100000
            WHEN policy_csl = '250/500' THEN 250000
            WHEN policy_csl = '500/1000' THEN 500000
            ELSE NULL
        END AS coverage_limit_bi,
        CASE 
            WHEN umbrella_limit < 0 THEN NULL  -- Fix data quality issue
            ELSE umbrella_limit 
        END AS umbrella_limit,
        months_as_customer,
        
        -- Insured information
        age,
        UPPER(insured_sex) AS gender,
        CASE 
            WHEN insured_relationship IN ('husband', 'wife') THEN 'Married'
            WHEN insured_relationship = 'unmarried' THEN 'Single'
            WHEN insured_relationship IN ('own-child', 'other-relative') THEN 'Single'
            ELSE 'Unknown'
        END AS marital_status,
        insured_education_level AS education_level,
        insured_occupation AS occupation,
        insured_hobbies AS hobbies,
        insured_zip AS zip_code,
        capital_gains AS capital_gains,
        capital_loss AS capital_loss,
        
        -- Vehicle information
        auto_year AS vehicle_year,
        auto_make AS vehicle_make,
        auto_model AS vehicle_model,
        EXTRACT(YEAR FROM CURRENT_DATE) - auto_year AS vehicle_age,
        
        -- Claim/Incident information
        incident_date::DATE AS incident_date,
        incident_type,
        collision_type,
        incident_severity,
        incident_state,
        incident_city,
        incident_location,
        incident_hour_of_the_day AS incident_hour,
        number_of_vehicles_involved AS vehicles_involved,
        CASE WHEN property_damage = 'YES' THEN TRUE ELSE FALSE END AS property_damage,
        bodily_injuries AS bodily_injuries_count,
        witnesses AS witnesses_count,
        CASE WHEN police_report_available = 'YES' THEN TRUE ELSE FALSE END AS police_report_available,
        authorities_contacted,
        
        -- Claim amounts
        total_claim_amount,
        injury_claim AS injury_claim_amount,
        property_claim AS property_claim_amount,
        vehicle_claim AS vehicle_claim_amount,
        
        -- Fraud indicator
        CASE WHEN fraud_reported = 'Y' THEN TRUE ELSE FALSE END AS fraud_reported,
        
        -- Metadata
        load_timestamp,
        source_file
        
    FROM source
    WHERE policy_number IS NOT NULL  -- Data quality filter
)

SELECT * FROM cleaned
