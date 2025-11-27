# Sample Data

This folder contains sample datasets (100 rows from each source) for demonstration purposes.

## Files

- **insurance_claims_sample.csv** - Sample from Customer A (100 of 1,000 records)
- **AutoBi_sample.csv** - Sample from Customer B (100 of 1,340 records)  
- **car_insurance_claim_sample.csv** - Sample from Customer C (100 of 10,302 records)

**Total**: 300 sample records (2.4% of full dataset)

## Full Datasets

The complete datasets are publicly available on Kaggle:

1. **Insurance Claims** (Customer A)
   - Source: https://www.kaggle.com/datasets/buntyshah/auto-insurance-claims-data
   - Size: 1,000 records, 40 columns

2. **Auto BI Claims** (Customer B)
   - Source: https://www.kaggle.com/datasets/xiaomengsun/auto-insurance
   - Size: 1,340 records, 9 columns

3. **Car Insurance Claims** (Customer C)
   - Source: https://www.kaggle.com/datasets/sagnik1511/car-insurance-data
   - Size: 10,302 records, 27 columns

## Usage

### For Testing

The sample data is sufficient to:
- Test the data loading scripts
- Verify database schema
- Run DBT transformations
- Explore the code structure

### For Full Analysis

To run the complete analysis:

1. Download full datasets from Kaggle (links above)
2. Place in `Dataset/` folder
3. Run data loaders: `python python/load_customer_*.py`
4. Execute DBT pipeline: `dbt run`

## Data Schema

See the main README.md for detailed schema documentation.

---

*Sample data provided for demonstration only. Full datasets required for complete analysis.*
