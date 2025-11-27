{{
    config(
        materialized='view',
        schema='staging'
    )
}}

/*
    Staging model for Customer C (car_insurance_claim.csv)
    Cleans and standardizes policy and claims data
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'customer_c_policies') }}
),

cleaned AS (
    SELECT
        -- Source tracking
        'customer_c' AS source_system,
        record_id::TEXT AS source_policy_number,
        
        -- Policy information
        time_in_force AS months_as_customer,
        
        -- Insured information
        CASE 
            WHEN age IS NOT NULL AND age >= 16 AND age <= 100 
            THEN age::INTEGER
            ELSE NULL
        END AS age,
        birth_date,
        CASE 
            WHEN UPPER(gender) = 'M' THEN 'M'
            WHEN UPPER(gender) = 'F' OR gender = 'z_F' THEN 'F'
            ELSE 'Unknown'
        END AS gender,
        CASE 
            WHEN marital_status = 'z_No' THEN 'Single'
            WHEN marital_status = 'Yes' THEN 'Married'
            ELSE 'Unknown'
        END AS marital_status,
        education,
        occupation,
        CASE 
            WHEN income LIKE '$%' THEN 
                REPLACE(REPLACE(income, '$', ''), ',', '')::DECIMAL
            ELSE NULL
        END AS income_annual,
        CASE 
            WHEN home_value LIKE '$%' THEN 
                REPLACE(REPLACE(home_value, '$', ''), ',', '')::DECIMAL
            WHEN home_value = '$0' THEN 0
            ELSE NULL
        END AS home_value,
        home_kids,
        years_on_job,
        travel_time AS travel_time_minutes,
        urbanicity,
        CASE WHEN parent1 = 'Yes' THEN TRUE ELSE FALSE END AS has_parent,
        
        -- Vehicle information
        CASE 
            WHEN bluebook_value LIKE '$%' THEN 
                REPLACE(REPLACE(bluebook_value, '$', ''), ',', '')::DECIMAL
            ELSE NULL
        END AS vehicle_value,
        car_type AS vehicle_type,
        CASE 
            WHEN car_age IS NOT NULL AND car_age >= 0 
            THEN car_age::INTEGER
            ELSE NULL  -- Fix negative car ages
        END AS vehicle_age,
        CASE WHEN red_car = 'yes' THEN TRUE ELSE FALSE END AS is_red_car,
        car_use,
        kids_driving,
        
        -- Claims history
        claim_frequency AS prior_claim_count,
        CASE 
            WHEN old_claim LIKE '$%' THEN 
                REPLACE(REPLACE(old_claim, '$', ''), ',', '')::DECIMAL
            WHEN old_claim = '$0' THEN 0
            ELSE NULL
        END AS prior_claim_total_amount,
        
        -- Current claim
        CASE WHEN claim_flag = 1 THEN TRUE ELSE FALSE END AS has_claim,
        CASE 
            WHEN claim_amount LIKE '$%' THEN 
                REPLACE(REPLACE(claim_amount, '$', ''), ',', '')::DECIMAL
            WHEN claim_amount = '$0' THEN 0
            ELSE NULL
        END AS claim_amount,
        
        -- Driving record
        mvr_points,
        CASE WHEN license_revoked = 'Yes' THEN TRUE ELSE FALSE END AS license_revoked,
        
        -- Metadata
        load_timestamp,
        source_file
        
    FROM source
    WHERE record_id IS NOT NULL
)

SELECT * FROM cleaned
