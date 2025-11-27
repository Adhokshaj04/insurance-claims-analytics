# Multi-Source Insurance Claims Analytics

**End-to-End Data Pipeline for P&C Insurance Analysis & Predictive Modeling**

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)](https://www.postgresql.org/)
[![DBT](https://img.shields.io/badge/DBT-1.10-orange)](https://www.getdbt.com/)
[![Python](https://img.shields.io/badge/Python-3.13-green)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 📋 Project Overview

This project demonstrates comprehensive data engineering and analytics capabilities through the harmonization of insurance claims data from three disparate sources, building a production-grade DBT transformation pipeline, and developing predictive models for claim severity classification.

**Use Case**: Multi-source data integration for property & casualty insurance analytics

---

## 🎯 Business Problem

Insurance companies often collect data from multiple systems with:
- Different schemas and data structures
- Inconsistent field names and data types
- Varying levels of data quality and completeness
- Missing or conflicting information across sources

**Solution**: Build a unified data warehouse that harmonizes all sources while maintaining data lineage, enabling reliable analytics and machine learning.

---

## 📊 Dataset Summary

| Source | Records | Columns | Description | Data Quality Score |
|--------|---------|---------|-------------|-------------------|
| **Source A** | 1,000 | 40 | Comprehensive claims with fraud indicators | 100% |
| **Source B** | 1,340 | 9 | Bodily injury claims | 62.5% |
| **Source C** | 10,302 | 27 | Policy and claims data | 50% |
| **TOTAL** | **12,642** | **76 unique fields** | **5,086 unified claims** | **63.1% average** |

*Data sourced from publicly available Kaggle insurance datasets*

---

## 🏗️ Architecture

```
Raw CSV Files
    ↓
PostgreSQL (Raw Layer)
    ↓
DBT Transformation Pipeline
    ├── Staging Layer - Data cleaning & standardization
    ├── Intermediate Layer - Source integration
    └── Marts Layer - Analytics-ready tables
    ↓
Analytics & Machine Learning
    ├── SQL analytics (10+ complex queries)
    ├── Feature engineering (25+ features)
    └── Predictive modeling (Claim severity)
```

---

## 🛠️ Technology Stack

- **Database**: PostgreSQL 15
- **Data Transformation**: DBT (Data Build Tool) 1.10
- **Programming**: Python 3.13
- **Data Processing**: pandas, numpy
- **Machine Learning**: scikit-learn
- **Visualization**: matplotlib, seaborn
- **Version Control**: Git

---

## 📁 Project Structure

```
insurance-claims-analytics/
├── Dataset/                          # Raw CSV files
│   ├── insurance_claims.csv
│   ├── AutoBi.csv
│   └── car_insurance_claim.csv
├── sql/
│   ├── schema_setup.sql             # Database schema
│   └── advanced_sql_queries.sql     # Analytics queries
├── python/
│   ├── load_customer_a.py           # Data loader scripts
│   ├── load_customer_b.py
│   ├── load_customer_c.py
│   └── ml_modeling.py               # ML pipeline
├── insurance_dbt/                    # DBT project
│   ├── models/
│   │   ├── staging/                 # Data cleaning
│   │   ├── intermediate/            # Data integration
│   │   └── marts/                   # Final analytics tables
│   └── dbt_project.yml
├── docs/                             # Documentation
│   ├── DBT_README.md
├── requirements.txt
└── README.md
```

---

## 🚀 Key Features

### 1. Multi-Source Data Harmonization
- Unified 76 unique fields from 3 different schemas
- Standardized data types (dates, amounts, categorical variables)
- Resolved naming conflicts and encoding differences
- Maintained complete data lineage
- Comprehensive data quality scoring

### 2. DBT Transformation Pipeline
- **Staging models**: Source-specific cleaning and standardization
- **Intermediate models**: Cross-source integration with type safety
- **Mart models**: Business-ready fact and dimension tables
- **Data tests**: Automated quality checks
- **Documentation**: Self-documenting data dictionary

### 3. Advanced SQL Analytics
Demonstrates expertise through:
- Window functions (RANK, DENSE_RANK, LAG, LEAD)
- Complex CTEs and subqueries
- Statistical analysis (percentiles, outlier detection)
- Time-series and cohort analysis
- Performance optimization techniques

### 4. Feature Engineering
Created 25+ derived features:
- Temporal patterns (seasonality, day-of-week effects)
- Risk indicators and composite scores
- Financial ratios and derived metrics
- Categorical encodings and binning
- Historical aggregations

### 5. Predictive Modeling
- **Objective**: Classify claim severity (4 categories)
- **Approach**: Compared Logistic Regression, Random Forest, Gradient Boosting
- **Performance**: 85%+ AUC-ROC for severe claim identification
- **Deployment-ready**: Includes data scaling, validation, and evaluation metrics

---

## 📈 Key Results

### Data Quality Insights
- Source A: 100% completeness (comprehensive data)
- Source B: 62.5% completeness (limited scope)
- Source C: 50% completeness (requires enrichment)

### Business Insights
- Average claim amount: $15,004
- Claim severity distribution: 14% severe, 8% significant, 67% moderate, 11% minor
- Top risk factor: Bodily injury claims (up to $1.07M)
- Fraud detection: 15% flagged in primary source

### Model Performance
- AUC-ROC: 0.85+
- Precision: 78% for severe claims
- Recall: 72% for severe claims
- Key predictors: Bodily injuries, prior claims history, vehicle age

---

## 🎓 Skills Demonstrated

### Technical Competencies
- **SQL**: Advanced queries, window functions, CTEs, optimization
- **Data Engineering**: ETL/ELT pipelines, data quality management
- **DBT**: Modern transformation patterns, testing, documentation
- **Python**: Data manipulation (pandas), machine learning (scikit-learn)
- **Statistics**: Outlier detection, cohort analysis, hypothesis testing
- **ML**: Feature engineering, model selection, evaluation

### Domain Knowledge
- P&C insurance concepts (coverage types, claims processes)
- Fraud detection patterns
- Risk assessment methodologies
- Regulatory and compliance considerations

### Professional Skills
- Technical documentation
- Code organization and best practices
- Performance optimization
- Scalable architecture design

---

## 🔧 Installation & Setup

### Prerequisites
- PostgreSQL 15+
- Python 3.13+
- DBT 1.10+

### Quick Start

1. **Clone repository**
```bash
git clone https://github.com/yourusername/insurance-claims-analytics.git
cd insurance-claims-analytics
```

2. **Set up database**
```bash
createdb insurance_analytics
psql -d insurance_analytics -f sql/schema_setup.sql
```

3. **Install Python dependencies**
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

4. **Load data**
```bash
python python/load_customer_a.py
python python/load_customer_b.py
python python/load_customer_c.py
```

5. **Run DBT pipeline**
```bash
cd insurance_dbt
dbt run
dbt test
```

6. **Execute ML pipeline**
```bash
python python/ml_modeling.py
```

---

## 📊 Example Queries

### Claims Distribution by Severity
```sql
SELECT 
    claim_severity_category,
    COUNT(*) as claim_count,
    ROUND(AVG(total_claim_amount), 2) as avg_amount,
    ROUND(STDDEV(total_claim_amount), 2) as stddev_amount
FROM insurance_staging_analytics.fct_claims
GROUP BY claim_severity_category
ORDER BY avg_amount DESC;
```

### Top Risk Factors
```sql
SELECT 
    age_group,
    vehicle_age_category,
    COUNT(*) as claims,
    ROUND(AVG(total_claim_amount), 2) as avg_claim
FROM insurance_staging_analytics.fct_claims
WHERE claim_severity_category = 'Severe'
GROUP BY age_group, vehicle_age_category
HAVING COUNT(*) > 5
ORDER BY avg_claim DESC;
```

### Data Quality Summary
```sql
SELECT 
    source_system,
    total_claims,
    ROUND(avg_data_quality_score, 1) as quality_score,
    ROUND(pct_has_incident_date, 1) as pct_complete_dates
FROM insurance_staging_analytics.mart_claims_summary
ORDER BY quality_score DESC;
```

---

## 📝 Project Highlights

### Challenge: Data Harmonization
Integrated three datasets with completely different structures:
- Varying column counts (40 vs 9 vs 27)
- Inconsistent data types and encodings
- Different levels of granularity
- Missing and conflicting values

### Solution: Layered Architecture
- Staging: Source-specific transformations
- Intermediate: Type-safe integration
- Marts: Business-optimized models
- Full data lineage preservation

### Impact: Actionable Analytics
- Unified view of 12,642 records
- 85%+ accuracy in predicting severe claims
- Identified key risk factors for targeting
- Production-ready data pipeline

---

## 🔮 Future Enhancements

- [ ] Real-time data ingestion (Kafka/Streaming)
- [ ] Incremental DBT models for scale
- [ ] Interactive dashboards (Tableau/PowerBI/Streamlit)
- [ ] ML model deployment as REST API
- [ ] Automated data quality monitoring
- [ ] A/B testing framework
- [ ] Time-series forecasting

---

## 👤 Author

**[Your Name]**  
Data Analyst | Data Engineer

📧 [your.email@example.com](mailto:your.email@example.com)  
💼 [LinkedIn](https://linkedin.com/in/yourprofile)  
🐙 [GitHub](https://github.com/yourusername)

---

## 📄 License

This project is licensed under the MIT License - see LICENSE file for details.

---

## 🙏 Acknowledgments

- Data sources: Kaggle Public Datasets
- DBT Community for transformation best practices
- Open-source ML community

---

*Built as a portfolio demonstration of end-to-end data analytics capabilities*

**⭐ Star this repo if you find it useful!**
