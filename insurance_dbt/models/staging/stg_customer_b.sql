{{
    config(
        materialized='view',
        schema='staging'
    )
}}

/*
    Staging model for Customer B (AutoBi.csv)
    Cleans and standardizes bodily injury claims data
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'customer_b_claims') }}
),

cleaned AS (
    SELECT
        -- Source tracking
        'customer_b' AS source_system,
        case_number::TEXT AS source_claim_number,
        
        -- Claim information (minimal data available)
        case_number,
        
        -- Insured/Claimant information
        CASE 
            WHEN claimant_age IS NOT NULL AND claimant_age >= 16 AND claimant_age <= 100 
            THEN claimant_age::INTEGER
            ELSE NULL
        END AS age,
        CASE 
            WHEN claimant_sex = 1 THEN 'M'
            WHEN claimant_sex = 2 THEN 'F'
            ELSE 'Unknown'
        END AS gender,
        CASE 
            WHEN marital_status = 1 THEN 'Single'
            WHEN marital_status = 2 THEN 'Married'
            WHEN marital_status = 3 THEN 'Widowed'
            WHEN marital_status = 4 THEN 'Divorced'
            ELSE 'Unknown'
        END AS marital_status,
        CASE 
            WHEN claimant_insured = 1 THEN TRUE
            WHEN claimant_insured = 2 THEN FALSE
            ELSE NULL
        END AS claimant_insured,
        
        -- Claim details
        CASE 
            WHEN attorney = 1 THEN FALSE
            WHEN attorney = 2 THEN TRUE
            ELSE NULL
        END AS attorney_involved,
        CASE 
            WHEN seatbelt = 1 THEN TRUE
            WHEN seatbelt = 2 THEN FALSE
            ELSE NULL
        END AS seatbelt_used,
        
        -- Claim amount (in thousands, convert to dollars)
        loss_amount * 1000 AS total_claim_amount,
        loss_amount * 1000 AS injury_claim_amount,  -- This is bodily injury only
        
        -- Metadata
        load_timestamp,
        source_file
        
    FROM source
    WHERE case_number IS NOT NULL
)

SELECT * FROM cleaned
