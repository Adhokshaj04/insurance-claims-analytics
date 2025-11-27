# Insurance Data DBT Project

## Overview

This DBT project transforms raw insurance data from three different customer systems into a unified, analytics-ready data model.

## Project Structure

```
insurance_dbt/
├── models/
│   ├── staging/
│   │   ├── schema.yml              # Source and test definitions
│   │   ├── stg_customer_a.sql      # Customer A staging model
│   │   ├── stg_customer_b.sql      # Customer B staging model
│   │   └── stg_customer_c.sql      # Customer C staging model
│   ├── intermediate/
│   │   └── int_claims_unified.sql  # Unified claims from all sources
│   └── marts/
│       ├── fct_claims.sql          # Claims fact table (analytics-ready)
│       └── mart_claims_summary.sql # Summary statistics by source
```

## Data Flow

```
RAW DATA (insurance_raw schema)
    ↓
STAGING (insurance_staging schema)
    - stg_customer_a: Clean Customer A data
    - stg_customer_b: Clean Customer B data
    - stg_customer_c: Clean Customer C data
    ↓
INTERMEDIATE (insurance_staging schema)
    - int_claims_unified: Union all sources
    ↓
MARTS (insurance_analytics schema)
    - fct_claims: Final claims fact table
    - mart_claims_summary: Aggregated statistics
```

## Data Sources

### Customer A (insurance_claims.csv)
- **Records**: 1,000
- **Columns**: 40
- **Contains**: Comprehensive claims data with fraud indicators
- **Key Features**: Rich incident details, vehicle info, multiple claim components

### Customer B (AutoBi.csv)
- **Records**: 1,340
- **Columns**: 9
- **Contains**: Bodily injury claims only
- **Key Features**: Attorney involvement, seatbelt usage, loss amounts

### Customer C (car_insurance_claim.csv)
- **Records**: 10,302 (2,746 with claims)
- **Columns**: 27
- **Contains**: Policy and claims data
- **Key Features**: Financial data, driving record, prior claims history

## Key Transformations

### Staging Layer
1. **Standardize field names** across sources
2. **Clean data types** (dates, amounts, booleans)
3. **Fix data quality issues** (negative values, invalid ages)
4. **Decode categorical variables** (gender, marital status)
5. **Parse currency strings** to numeric values

### Intermediate Layer
1. **Union all claims** from three sources
2. **Handle schema differences** with NULL values for missing fields
3. **Maintain source tracking** for full lineage

### Marts Layer
1. **Generate unique IDs** for cross-system analysis
2. **Create derived fields** (age groups, severity categories)
3. **Calculate metrics** (claim-to-premium ratio, data quality score)
4. **Enrich with temporal dimensions** (year, month, day of week)

## Data Quality

### Quality Checks
- NOT NULL constraints on key fields
- Accepted values tests for source_system
- Age validation (16-100 years)
- Amount validation (>= 0)

### Data Quality Score
Each claim gets a score (0-100) based on field completeness:
- Age: 10 points
- Gender: 10 points
- Incident date: 15 points
- Claim amount: 20 points
- Vehicle year: 10 points
- Policy state: 10 points
- Incident type: 15 points
- Marital status: 10 points

## Running the Project

### Build all models
```bash
dbt run
```

### Run specific models
```bash
dbt run --select stg_customer_a
dbt run --select int_claims_unified+
dbt run --select fct_claims
```

### Run tests
```bash
dbt test
```

### Generate documentation
```bash
dbt docs generate
dbt docs serve
```

## Key Metrics

After running, you can query these key metrics:

```sql
-- Overall statistics
SELECT * FROM insurance_analytics.mart_claims_summary
WHERE source_system = 'ALL_SOURCES';

-- Top 10 highest claims
SELECT 
    claim_id,
    source_system,
    total_claim_amount,
    incident_type,
    claim_severity_category
FROM insurance_analytics.fct_claims
ORDER BY total_claim_amount DESC
LIMIT 10;

-- Fraud rate by source
SELECT 
    source_system,
    fraud_count,
    total_claims,
    fraud_rate
FROM insurance_analytics.mart_claims_summary
ORDER BY fraud_rate DESC;
```

## Data Lineage

All models maintain full lineage:
- `source_system`: Which customer the data came from
- `source_policy_number`: Original policy identifier
- `source_claim_number`: Original claim identifier
- `load_timestamp`: When data was loaded
- `created_at`: When analytics record was created

## Interview Talking Points

**Data Harmonization**: "I harmonized data from three disparate sources with different schemas (40, 9, and 27 columns) into a unified data model."

**Data Quality**: "I implemented data quality scoring and validation, fixing issues like negative values and inconsistent encoding across sources."

**Scalability**: "The DBT pipeline is modular and reusable - adding a fourth customer source just requires a new staging model and minimal changes."

**Business Value**: "The unified model enables cross-customer analytics that weren't possible before, while maintaining full data lineage for auditability."

## Next Steps

1. Add more intermediate models for specific analyses
2. Create dimension tables (dim_customer, dim_vehicle, dim_policy)
3. Build incremental models for ongoing data loads
4. Add data quality monitoring
5. Create ML features for predictive modeling
