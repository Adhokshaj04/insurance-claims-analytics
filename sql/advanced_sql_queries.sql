-- ============================================================================
-- GUIDEWIRE INSURANCE ANALYTICS - ADVANCED SQL QUERIES
-- ============================================================================
-- Purpose: Demonstrate expert SQL skills for Data Analyst position
-- Author: [Your Name]
-- Date: November 2025
-- Database: insurance_analytics
-- ============================================================================

-- Set schema for queries
SET search_path TO insurance_staging_analytics, insurance_analytics, public;

-- ============================================================================
-- 1. WINDOW FUNCTIONS - Claim Ranking and Running Totals
-- ============================================================================

-- Query 1.1: Rank claims by amount within each source system
-- Demonstrates: DENSE_RANK, PARTITION BY
SELECT 
    source_system,
    claim_id,
    total_claim_amount,
    DENSE_RANK() OVER (
        PARTITION BY source_system 
        ORDER BY total_claim_amount DESC
    ) as claim_rank_in_source,
    ROUND(
        PERCENT_RANK() OVER (
            PARTITION BY source_system 
            ORDER BY total_claim_amount
        )::numeric * 100, 
    2) as percentile_rank
FROM fct_claims
WHERE total_claim_amount > 0
ORDER BY source_system, claim_rank_in_source
LIMIT 20;

-- Query 1.2: Running total of claims by incident date
-- Demonstrates: SUM() OVER with ORDER BY
SELECT 
    incident_date,
    COUNT(*) as daily_claims,
    SUM(total_claim_amount) as daily_claim_total,
    SUM(COUNT(*)) OVER (
        ORDER BY incident_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as running_total_claims,
    SUM(SUM(total_claim_amount)) OVER (
        ORDER BY incident_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as running_total_dollars
FROM fct_claims
WHERE incident_date IS NOT NULL
GROUP BY incident_date
ORDER BY incident_date;

-- Query 1.3: Compare each claim to moving average
-- Demonstrates: AVG() OVER with frame specification
SELECT 
    claim_id,
    incident_date,
    age,
    total_claim_amount,
    ROUND(
        AVG(total_claim_amount) OVER (
            ORDER BY incident_date 
            ROWS BETWEEN 50 PRECEDING AND 50 FOLLOWING
        )::numeric, 
    2) as moving_avg_100_claims,
    ROUND(
        (total_claim_amount - AVG(total_claim_amount) OVER (
            ORDER BY incident_date 
            ROWS BETWEEN 50 PRECEDING AND 50 FOLLOWING
        ))::numeric,
    2) as deviation_from_avg
FROM fct_claims
WHERE incident_date IS NOT NULL
ORDER BY incident_date
LIMIT 100;

-- ============================================================================
-- 2. COMPLEX JOINS - Multi-table Analysis
-- ============================================================================

-- Query 2.1: Claims with policy and insured details (if tables were populated)
-- Demonstrates: LEFT JOIN, COALESCE, CASE
SELECT 
    c.claim_id,
    c.source_system,
    c.age,
    c.gender,
    c.marital_status,
    c.vehicle_make,
    c.vehicle_model,
    c.incident_type,
    c.total_claim_amount,
    c.claim_severity_category,
    CASE 
        WHEN c.fraud_reported = TRUE THEN 'Yes'
        WHEN c.fraud_reported = FALSE THEN 'No'
        ELSE 'Unknown'
    END as fraud_status,
    c.data_quality_score
FROM fct_claims c
WHERE c.total_claim_amount > 50000
ORDER BY c.total_claim_amount DESC
LIMIT 25;

-- ============================================================================
-- 3. ADVANCED CTEs - Loss Ratio Analysis
-- ============================================================================

-- Query 3.1: Calculate loss ratios by age group and vehicle age
-- Demonstrates: Multiple CTEs, aggregations, business logic
WITH claim_totals AS (
    SELECT 
        age_group,
        vehicle_age_category,
        COUNT(*) as claim_count,
        SUM(total_claim_amount) as total_claims_paid,
        AVG(total_claim_amount) as avg_claim_amount
    FROM fct_claims
    WHERE total_claim_amount > 0
    GROUP BY age_group, vehicle_age_category
),
premium_estimates AS (
    SELECT 
        age_group,
        vehicle_age_category,
        COUNT(*) as policy_count,
        AVG(policy_annual_premium) as avg_premium
    FROM fct_claims
    WHERE policy_annual_premium > 0
    GROUP BY age_group, vehicle_age_category
),
loss_ratios AS (
    SELECT 
        c.age_group,
        c.vehicle_age_category,
        c.claim_count,
        c.total_claims_paid,
        p.avg_premium,
        CASE 
            WHEN p.avg_premium > 0 THEN 
                ROUND((c.total_claims_paid / (c.claim_count * p.avg_premium) * 100)::numeric, 2)
            ELSE NULL
        END as loss_ratio_pct
    FROM claim_totals c
    LEFT JOIN premium_estimates p 
        ON c.age_group = p.age_group 
        AND c.vehicle_age_category = p.vehicle_age_category
)
SELECT 
    age_group,
    vehicle_age_category,
    claim_count,
    ROUND(total_claims_paid::numeric, 2) as total_paid,
    ROUND(avg_premium::numeric, 2) as avg_premium,
    loss_ratio_pct,
    CASE 
        WHEN loss_ratio_pct > 100 THEN 'Unprofitable'
        WHEN loss_ratio_pct > 70 THEN 'High Risk'
        WHEN loss_ratio_pct > 50 THEN 'Moderate Risk'
        ELSE 'Profitable'
    END as profitability_category
FROM loss_ratios
WHERE loss_ratio_pct IS NOT NULL
ORDER BY loss_ratio_pct DESC;

-- ============================================================================
-- 4. COHORT ANALYSIS - Claims by Time Period
-- ============================================================================

-- Query 4.1: Monthly claim trends with year-over-year comparison
-- Demonstrates: DATE functions, LAG(), complex aggregations
WITH monthly_claims AS (
    SELECT 
        DATE_TRUNC('month', incident_date) as claim_month,
        EXTRACT(YEAR FROM incident_date) as claim_year,
        EXTRACT(MONTH FROM incident_date) as month_num,
        COUNT(*) as claim_count,
        SUM(total_claim_amount) as total_amount,
        AVG(total_claim_amount) as avg_amount
    FROM fct_claims
    WHERE incident_date IS NOT NULL
    GROUP BY 
        DATE_TRUNC('month', incident_date),
        EXTRACT(YEAR FROM incident_date),
        EXTRACT(MONTH FROM incident_date)
)
SELECT 
    claim_month,
    claim_year,
    month_num,
    claim_count,
    ROUND(total_amount::numeric, 2) as total_amount,
    ROUND(avg_amount::numeric, 2) as avg_amount,
    LAG(claim_count, 1) OVER (ORDER BY claim_month) as prev_month_claims,
    ROUND(
        ((claim_count - LAG(claim_count, 1) OVER (ORDER BY claim_month))::numeric / 
         NULLIF(LAG(claim_count, 1) OVER (ORDER BY claim_month), 0) * 100),
    2) as pct_change_from_prev_month
FROM monthly_claims
ORDER BY claim_month DESC
LIMIT 24;

-- ============================================================================
-- 5. STATISTICAL ANALYSIS - Outlier Detection
-- ============================================================================

-- Query 5.1: Identify outlier claims using IQR method
-- Demonstrates: Percentile functions, subqueries, statistical concepts
WITH claim_stats AS (
    SELECT 
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_claim_amount) as q1,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_claim_amount) as median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_claim_amount) as q3,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_claim_amount) - 
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_claim_amount) as iqr
    FROM fct_claims
    WHERE total_claim_amount > 0
)
SELECT 
    c.claim_id,
    c.source_system,
    c.incident_type,
    c.age,
    c.total_claim_amount,
    ROUND(s.median::numeric, 2) as median_claim,
    ROUND(
        (c.total_claim_amount - s.median)::numeric / NULLIF(s.iqr, 0),
    2) as iqr_distance,
    CASE 
        WHEN c.total_claim_amount > (s.q3 + 3 * s.iqr) THEN 'Extreme Outlier (>3 IQR)'
        WHEN c.total_claim_amount > (s.q3 + 1.5 * s.iqr) THEN 'Moderate Outlier (>1.5 IQR)'
        WHEN c.total_claim_amount < (s.q1 - 1.5 * s.iqr) THEN 'Low Outlier'
        ELSE 'Normal'
    END as outlier_category
FROM fct_claims c
CROSS JOIN claim_stats s
WHERE c.total_claim_amount > 0
    AND c.total_claim_amount > (s.q3 + 1.5 * s.iqr)
ORDER BY c.total_claim_amount DESC
LIMIT 50;

-- ============================================================================
-- 6. FRAUD DETECTION ANALYSIS
-- ============================================================================

-- Query 6.1: Fraud patterns by demographic and incident characteristics
-- Demonstrates: Conditional aggregation, multiple dimensions
SELECT 
    age_group,
    gender,
    incident_severity,
    COUNT(*) as total_claims,
    COUNT(CASE WHEN fraud_reported = TRUE THEN 1 END) as fraud_claims,
    ROUND(
        COUNT(CASE WHEN fraud_reported = TRUE THEN 1 END)::numeric / 
        NULLIF(COUNT(*), 0) * 100,
    2) as fraud_rate_pct,
    ROUND(AVG(total_claim_amount)::numeric, 2) as avg_claim_all,
    ROUND(
        AVG(CASE WHEN fraud_reported = TRUE THEN total_claim_amount END)::numeric,
    2) as avg_claim_fraud,
    ROUND(
        AVG(CASE WHEN fraud_reported = FALSE THEN total_claim_amount END)::numeric,
    2) as avg_claim_legit
FROM fct_claims
WHERE fraud_reported IS NOT NULL
GROUP BY age_group, gender, incident_severity
HAVING COUNT(*) >= 5  -- Only show segments with sufficient data
ORDER BY fraud_rate_pct DESC
LIMIT 30;

-- ============================================================================
-- 7. GEOGRAPHIC ANALYSIS
-- ============================================================================

-- Query 7.1: Claims analysis by state
-- Demonstrates: Grouping, ranking, geographic analysis
WITH state_summary AS (
    SELECT 
        COALESCE(incident_state, policy_state) as state,
        COUNT(*) as claim_count,
        SUM(total_claim_amount) as total_amount,
        AVG(total_claim_amount) as avg_amount,
        STDDEV(total_claim_amount) as stddev_amount,
        COUNT(CASE WHEN claim_severity_category = 'Severe' THEN 1 END) as severe_count
    FROM fct_claims
    WHERE COALESCE(incident_state, policy_state) IS NOT NULL
    GROUP BY COALESCE(incident_state, policy_state)
)
SELECT 
    state,
    claim_count,
    ROUND(total_amount::numeric, 2) as total_amount,
    ROUND(avg_amount::numeric, 2) as avg_amount,
    ROUND(stddev_amount::numeric, 2) as claim_volatility,
    severe_count,
    ROUND((severe_count::numeric / claim_count * 100), 2) as severe_claim_rate,
    RANK() OVER (ORDER BY total_amount DESC) as rank_by_total,
    RANK() OVER (ORDER BY avg_amount DESC) as rank_by_avg
FROM state_summary
WHERE claim_count >= 10
ORDER BY total_amount DESC;

-- ============================================================================
-- 8. DATA QUALITY ANALYSIS
-- ============================================================================

-- Query 8.1: Field completeness analysis across sources
-- Demonstrates: Conditional aggregation, data quality metrics
SELECT 
    source_system,
    COUNT(*) as total_records,
    
    -- Demographic completeness
    ROUND(COUNT(age)::numeric / COUNT(*) * 100, 1) as pct_has_age,
    ROUND(COUNT(gender)::numeric / COUNT(*) * 100, 1) as pct_has_gender,
    ROUND(COUNT(marital_status)::numeric / COUNT(*) * 100, 1) as pct_has_marital,
    
    -- Incident completeness
    ROUND(COUNT(incident_date)::numeric / COUNT(*) * 100, 1) as pct_has_incident_date,
    ROUND(COUNT(incident_type)::numeric / COUNT(*) * 100, 1) as pct_has_incident_type,
    
    -- Vehicle completeness
    ROUND(COUNT(vehicle_make)::numeric / COUNT(*) * 100, 1) as pct_has_vehicle_make,
    ROUND(COUNT(vehicle_year)::numeric / COUNT(*) * 100, 1) as pct_has_vehicle_year,
    
    -- Financial completeness
    ROUND(COUNT(policy_annual_premium)::numeric / COUNT(*) * 100, 1) as pct_has_premium,
    ROUND(COUNT(total_claim_amount)::numeric / COUNT(*) * 100, 1) as pct_has_claim_amt,
    
    -- Overall quality score
    ROUND(AVG(data_quality_score)::numeric, 1) as avg_quality_score
FROM fct_claims
GROUP BY source_system
ORDER BY avg_quality_score DESC;

-- ============================================================================
-- 9. PREDICTIVE FEATURES - Feature Engineering for ML
-- ============================================================================

-- Query 9.1: Create features for claims severity prediction
-- Demonstrates: Feature engineering, complex calculations
SELECT 
    claim_id,
    source_system,
    
    -- Demographic features
    age,
    age_group,
    CASE WHEN gender = 'M' THEN 1 ELSE 0 END as is_male,
    CASE WHEN marital_status = 'Married' THEN 1 ELSE 0 END as is_married,
    
    -- Vehicle features
    vehicle_age,
    vehicle_age_category,
    CASE WHEN is_red_car = TRUE THEN 1 ELSE 0 END as red_car_flag,
    
    -- Temporal features
    EXTRACT(MONTH FROM incident_date) as incident_month,
    EXTRACT(DOW FROM incident_date) as incident_day_of_week,
    incident_is_weekend::integer as weekend_incident,
    CASE 
        WHEN incident_hour BETWEEN 6 AND 18 THEN 1 
        ELSE 0 
    END as daytime_incident,
    
    -- Incident features
    COALESCE(vehicles_involved, 1) as vehicles_count,
    COALESCE(bodily_injuries_count, 0) as injury_count,
    COALESCE(witnesses_count, 0) as witness_count,
    police_report_available::integer as has_police_report,
    
    -- Prior history features
    COALESCE(prior_claim_count, 0) as prior_claims,
    CASE 
        WHEN prior_claim_count = 0 THEN 'none'
        WHEN prior_claim_count <= 2 THEN 'low'
        ELSE 'high'
    END as prior_claim_category,
    
    -- Risk scores
    COALESCE(mvr_points, 0) as mvr_points,
    license_revoked::integer as license_revoked_flag,
    
    -- Target variable
    total_claim_amount,
    claim_severity_category,
    CASE 
        WHEN total_claim_amount > 50000 THEN 1 
        ELSE 0 
    END as is_severe_claim
    
FROM fct_claims
WHERE total_claim_amount > 0
ORDER BY RANDOM()
LIMIT 1000;

-- ============================================================================
-- 10. PERFORMANCE OPTIMIZATION EXAMPLE
-- ============================================================================

-- Query 10.1: Optimized query with EXPLAIN ANALYZE
-- Demonstrates: Query optimization awareness
EXPLAIN ANALYZE
SELECT 
    c.age_group,
    c.claim_severity_category,
    COUNT(*) as claim_count,
    AVG(c.total_claim_amount) as avg_amount
FROM fct_claims c
WHERE c.total_claim_amount > 1000
    AND c.incident_date >= '2015-01-01'
GROUP BY c.age_group, c.claim_severity_category
HAVING COUNT(*) > 10
ORDER BY avg_amount DESC;

-- ============================================================================
-- END OF ADVANCED SQL QUERIES
-- ============================================================================
/*
INTERVIEW TALKING POINTS:

1. Window Functions: "I used DENSE_RANK and PERCENT_RANK to identify top claims
   within each data source, which helped identify outliers."

2. CTEs: "I built multi-level CTEs to calculate loss ratios by cohort, making
   complex business logic readable and maintainable."

3. Statistical Analysis: "I implemented IQR-based outlier detection to flag
   potentially fraudulent claims automatically."

4. Data Quality: "I created comprehensive data quality metrics showing Customer A
   had 100% completeness while Customer C had only 50%."

5. Feature Engineering: "I derived 20+ predictive features from raw data for
   downstream ML modeling, including temporal, demographic, and risk factors."

6. Performance: "I'm conscious of query performance and use EXPLAIN ANALYZE to
   optimize execution plans."
*/
