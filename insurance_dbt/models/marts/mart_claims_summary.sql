{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

/*
    Mart: Claims Summary by Source
    Aggregated statistics for comparing data sources
*/

WITH claims AS (
    SELECT * FROM {{ ref('fct_claims') }}
),

source_summary AS (
    SELECT
        source_system,
        
        -- Record counts
        COUNT(*) AS total_claims,
        COUNT(DISTINCT source_policy_number) AS unique_policies,
        
        -- Claim amounts
        SUM(total_claim_amount) AS total_claim_dollars,
        AVG(total_claim_amount) AS avg_claim_amount,
        MIN(total_claim_amount) AS min_claim_amount,
        MAX(total_claim_amount) AS max_claim_amount,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_claim_amount) AS median_claim_amount,
        
        -- Demographics
        AVG(age) AS avg_age,
        COUNT(CASE WHEN gender = 'M' THEN 1 END)::FLOAT / NULLIF(COUNT(gender), 0) * 100 AS pct_male,
        COUNT(CASE WHEN marital_status = 'Married' THEN 1 END)::FLOAT / NULLIF(COUNT(marital_status), 0) * 100 AS pct_married,
        
        -- Vehicle info
        AVG(vehicle_age) AS avg_vehicle_age,
        
        -- Fraud
        COUNT(CASE WHEN fraud_reported = TRUE THEN 1 END) AS fraud_count,
        COUNT(CASE WHEN fraud_reported = TRUE THEN 1 END)::FLOAT / NULLIF(COUNT(*), 0) * 100 AS fraud_rate,
        
        -- Severity
        COUNT(CASE WHEN claim_severity_category = 'Severe' THEN 1 END) AS severe_claims_count,
        COUNT(CASE WHEN incident_severity = 'Total Loss' THEN 1 END) AS total_loss_count,
        
        -- Data quality
        AVG(data_quality_score) AS avg_data_quality_score,
        MIN(data_quality_score) AS min_data_quality_score,
        MAX(data_quality_score) AS max_data_quality_score,
        
        -- Completeness metrics
        COUNT(CASE WHEN age IS NOT NULL THEN 1 END)::FLOAT / NULLIF(COUNT(*), 0) * 100 AS pct_has_age,
        COUNT(CASE WHEN incident_date IS NOT NULL THEN 1 END)::FLOAT / NULLIF(COUNT(*), 0) * 100 AS pct_has_incident_date,
        COUNT(CASE WHEN vehicle_year IS NOT NULL THEN 1 END)::FLOAT / NULLIF(COUNT(*), 0) * 100 AS pct_has_vehicle_info,
        COUNT(CASE WHEN policy_annual_premium IS NOT NULL THEN 1 END)::FLOAT / NULLIF(COUNT(*), 0) * 100 AS pct_has_premium,
        
        -- Metadata
        MIN(load_timestamp) AS earliest_load,
        MAX(load_timestamp) AS latest_load,
        CURRENT_TIMESTAMP AS summary_created_at,
        
        -- Sort order helper
        CASE 
            WHEN source_system = 'customer_a' THEN 1
            WHEN source_system = 'customer_b' THEN 2
            WHEN source_system = 'customer_c' THEN 3
        END AS sort_order
        
    FROM claims
    GROUP BY source_system
),

overall_summary AS (
    SELECT
        'ALL_SOURCES' AS source_system,
        COUNT(*) AS total_claims,
        COUNT(DISTINCT source_policy_number) AS unique_policies,
        SUM(total_claim_amount) AS total_claim_dollars,
        AVG(total_claim_amount) AS avg_claim_amount,
        MIN(total_claim_amount) AS min_claim_amount,
        MAX(total_claim_amount) AS max_claim_amount,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_claim_amount) AS median_claim_amount,
        AVG(age) AS avg_age,
        COUNT(CASE WHEN gender = 'M' THEN 1 END)::FLOAT / NULLIF(COUNT(gender), 0) * 100 AS pct_male,
        COUNT(CASE WHEN marital_status = 'Married' THEN 1 END)::FLOAT / NULLIF(COUNT(marital_status), 0) * 100 AS pct_married,
        AVG(vehicle_age) AS avg_vehicle_age,
        COUNT(CASE WHEN fraud_reported = TRUE THEN 1 END) AS fraud_count,
        COUNT(CASE WHEN fraud_reported = TRUE THEN 1 END)::FLOAT / NULLIF(COUNT(*), 0) * 100 AS fraud_rate,
        COUNT(CASE WHEN claim_severity_category = 'Severe' THEN 1 END) AS severe_claims_count,
        COUNT(CASE WHEN incident_severity = 'Total Loss' THEN 1 END) AS total_loss_count,
        AVG(data_quality_score) AS avg_data_quality_score,
        MIN(data_quality_score) AS min_data_quality_score,
        MAX(data_quality_score) AS max_data_quality_score,
        COUNT(CASE WHEN age IS NOT NULL THEN 1 END)::FLOAT / NULLIF(COUNT(*), 0) * 100 AS pct_has_age,
        COUNT(CASE WHEN incident_date IS NOT NULL THEN 1 END)::FLOAT / NULLIF(COUNT(*), 0) * 100 AS pct_has_incident_date,
        COUNT(CASE WHEN vehicle_year IS NOT NULL THEN 1 END)::FLOAT / NULLIF(COUNT(*), 0) * 100 AS pct_has_vehicle_info,
        COUNT(CASE WHEN policy_annual_premium IS NOT NULL THEN 1 END)::FLOAT / NULLIF(COUNT(*), 0) * 100 AS pct_has_premium,
        MIN(load_timestamp) AS earliest_load,
        MAX(load_timestamp) AS latest_load,
        CURRENT_TIMESTAMP AS summary_created_at,
        4 AS sort_order  -- Sort order for ALL_SOURCES
    FROM claims
),

combined AS (
    SELECT * FROM source_summary
    UNION ALL
    SELECT * FROM overall_summary
)

SELECT 
    source_system,
    total_claims,
    unique_policies,
    total_claim_dollars,
    avg_claim_amount,
    min_claim_amount,
    max_claim_amount,
    median_claim_amount,
    avg_age,
    pct_male,
    pct_married,
    avg_vehicle_age,
    fraud_count,
    fraud_rate,
    severe_claims_count,
    total_loss_count,
    avg_data_quality_score,
    min_data_quality_score,
    max_data_quality_score,
    pct_has_age,
    pct_has_incident_date,
    pct_has_vehicle_info,
    pct_has_premium,
    earliest_load,
    latest_load,
    summary_created_at
FROM combined
ORDER BY sort_order
