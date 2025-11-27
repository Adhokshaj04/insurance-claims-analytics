{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

/*
    Mart: Claims Analysis
    Final analytics-ready table for claims reporting
*/

WITH claims AS (
    SELECT * FROM {{ ref('int_claims_unified') }}
),

enriched AS (
    SELECT
        -- Generate unique claim ID
        source_system || '_' || source_claim_number AS claim_id,
        
        -- Source info
        source_system,
        source_policy_number,
        source_claim_number,
        
        -- Policy information
        policy_effective_date,
        policy_state,
        policy_annual_premium,
        policy_deductible,
        coverage_limit_bi,
        umbrella_limit,
        months_as_customer,
        
        -- Insured demographics
        age,
        CASE 
            WHEN age < 25 THEN '18-24'
            WHEN age < 35 THEN '25-34'
            WHEN age < 45 THEN '35-44'
            WHEN age < 55 THEN '45-54'
            WHEN age < 65 THEN '55-64'
            ELSE '65+'
        END AS age_group,
        gender,
        marital_status,
        education_level,
        occupation,
        income_annual,
        home_value,
        zip_code,
        urbanicity,
        
        -- Vehicle information
        vehicle_year,
        vehicle_make,
        vehicle_model,
        vehicle_age,
        CASE 
            WHEN vehicle_age < 3 THEN 'New (0-2)'
            WHEN vehicle_age < 6 THEN 'Recent (3-5)'
            WHEN vehicle_age < 11 THEN 'Older (6-10)'
            ELSE 'Very Old (11+)'
        END AS vehicle_age_category,
        vehicle_type,
        vehicle_value,
        is_red_car,
        
        -- Incident details
        incident_date,
        EXTRACT(YEAR FROM incident_date) AS incident_year,
        EXTRACT(MONTH FROM incident_date) AS incident_month,
        EXTRACT(DOW FROM incident_date) AS incident_day_of_week,
        CASE 
            WHEN EXTRACT(DOW FROM incident_date) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END AS incident_is_weekend,
        incident_type,
        collision_type,
        incident_severity,
        incident_state,
        incident_city,
        incident_hour,
        CASE 
            WHEN incident_hour BETWEEN 6 AND 11 THEN 'Morning'
            WHEN incident_hour BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN incident_hour BETWEEN 18 AND 21 THEN 'Evening'
            ELSE 'Night'
        END AS time_of_day,
        vehicles_involved,
        bodily_injuries_count,
        witnesses_count,
        police_report_available,
        authorities_contacted,
        attorney_involved,
        seatbelt_used,
        property_damage,
        
        -- Claim amounts
        total_claim_amount,
        injury_claim_amount,
        property_claim_amount,
        vehicle_claim_amount,
        
        -- Claim severity classification
        CASE 
            WHEN total_claim_amount < 1000 THEN 'Minor'
            WHEN total_claim_amount < 10000 THEN 'Moderate'
            WHEN total_claim_amount < 50000 THEN 'Significant'
            ELSE 'Severe'
        END AS claim_severity_category,
        
        -- Prior claims history
        prior_claim_count,
        prior_claim_total_amount,
        CASE 
            WHEN prior_claim_count = 0 THEN 'None'
            WHEN prior_claim_count = 1 THEN 'One'
            WHEN prior_claim_count <= 3 THEN 'Few (2-3)'
            ELSE 'Many (4+)'
        END AS prior_claims_category,
        
        -- Driving record
        mvr_points,
        license_revoked,
        CASE 
            WHEN mvr_points = 0 THEN 'Clean'
            WHEN mvr_points <= 3 THEN 'Minor Issues'
            ELSE 'Major Issues'
        END AS driving_record_category,
        
        -- Fraud
        fraud_reported,
        
        -- Calculated fields
        CASE 
            WHEN policy_annual_premium > 0 
            THEN total_claim_amount / policy_annual_premium 
            ELSE NULL 
        END AS claim_to_premium_ratio,
        
        -- Data quality score
        CASE 
            WHEN age IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN gender IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN incident_date IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN total_claim_amount IS NOT NULL THEN 20 ELSE 0 END +
            CASE WHEN vehicle_year IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN policy_state IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN incident_type IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN marital_status IS NOT NULL THEN 10 ELSE 0 END
        AS data_quality_score,
        
        -- Metadata
        load_timestamp,
        CURRENT_TIMESTAMP AS created_at
        
    FROM claims
)

SELECT * FROM enriched
