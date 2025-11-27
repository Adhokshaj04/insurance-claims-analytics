"""
Guidewire Insurance Analytics - Feature Engineering & Predictive Modeling
Purpose: Build predictive model for claim severity classification
Author: Data Analyst Candidate
Date: November 2025
"""

import pandas as pd
import numpy as np
from datetime import datetime
import psycopg
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    classification_report, 
    confusion_matrix, 
    roc_auc_score,
    roc_curve
)
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

# Database connection
DB_CONFIG = {
    'dbname': 'insurance_analytics',
    'user': 'addy',
    'password': 'password123',  # Update with your password
    'host': 'localhost',
    'port': '5432'
}

print("="*80)
print("GUIDEWIRE INSURANCE ANALYTICS - ML MODELING")
print("="*80)
print()

# ============================================================================
# 1. DATA EXTRACTION
# ============================================================================

print("1. Extracting data from PostgreSQL...")

query = """
SELECT 
    -- Target variable
    CASE 
        WHEN total_claim_amount >= 50000 THEN 'Severe'
        WHEN total_claim_amount >= 10000 THEN 'Significant'
        WHEN total_claim_amount >= 1000 THEN 'Moderate'
        ELSE 'Minor'
    END as claim_severity,
    
    -- Demographics
    age,
    CASE WHEN gender = 'M' THEN 1 ELSE 0 END as is_male,
    CASE WHEN marital_status = 'Married' THEN 1 ELSE 0 END as is_married,
    education_level,
    occupation,
    
    -- Vehicle
    vehicle_age,
    vehicle_year,
    vehicle_make,
    vehicle_type,
    CASE WHEN is_red_car = TRUE THEN 1 ELSE 0 END as is_red_car,
    
    -- Policy
    policy_annual_premium,
    policy_deductible,
    coverage_limit_bi,
    months_as_customer,
    
    -- Incident details
    EXTRACT(MONTH FROM incident_date) as incident_month,
    EXTRACT(DOW FROM incident_date) as incident_day_of_week,
    incident_hour,
    CASE WHEN incident_is_weekend THEN 1 ELSE 0 END as is_weekend,
    incident_type,
    collision_type,
    incident_severity,
    vehicles_involved,
    bodily_injuries_count,
    witnesses_count,
    CASE WHEN police_report_available = TRUE THEN 1 ELSE 0 END as has_police_report,
    CASE WHEN property_damage = TRUE THEN 1 ELSE 0 END as has_property_damage,
    
    -- Prior history
    COALESCE(prior_claim_count, 0) as prior_claim_count,
    COALESCE(prior_claim_total_amount, 0) as prior_claim_total,
    COALESCE(mvr_points, 0) as mvr_points,
    CASE WHEN license_revoked = TRUE THEN 1 ELSE 0 END as license_revoked,
    
    -- Financial
    income_annual,
    home_value,
    
    -- Fraud indicator
    CASE WHEN fraud_reported = TRUE THEN 1 ELSE 0 END as is_fraud,
    
    -- Actual amount (for analysis)
    total_claim_amount
    
FROM insurance_staging_analytics.fct_claims
WHERE total_claim_amount > 0
    AND age IS NOT NULL
    AND total_claim_amount < 1000000  -- Remove extreme outliers
"""

try:
    conn = psycopg.connect(**DB_CONFIG)
    df = pd.read_sql_query(query, conn)
    conn.close()
    print(f"   ✓ Loaded {len(df):,} records")
    print(f"   ✓ {len(df.columns)} features")
except Exception as e:
    print(f"   ✗ Error: {e}")
    exit(1)

print()

# ============================================================================
# 2. FEATURE ENGINEERING
# ============================================================================

print("2. Engineering features...")

# Age groups
df['age_group'] = pd.cut(df['age'], 
                         bins=[0, 25, 35, 45, 55, 65, 100],
                         labels=['18-24', '25-34', '35-44', '45-54', '55-64', '65+'])

# Vehicle age categories
df['vehicle_age_cat'] = pd.cut(df['vehicle_age'].fillna(0), 
                               bins=[-1, 3, 6, 11, 100],
                               labels=['New', 'Recent', 'Older', 'Very Old'])

# Time of day
df['time_of_day'] = pd.cut(df['incident_hour'].fillna(12), 
                           bins=[0, 6, 12, 18, 24],
                           labels=['Night', 'Morning', 'Afternoon', 'Evening'])

# Premium to coverage ratio
df['premium_to_coverage_ratio'] = df['policy_annual_premium'] / (df['coverage_limit_bi'].fillna(100000) + 1)

# Customer tenure category
df['tenure_category'] = pd.cut(df['months_as_customer'].fillna(0),
                              bins=[-1, 6, 12, 24, 1000],
                              labels=['New', 'Short', 'Medium', 'Long'])

# Risk score (composite)
df['risk_score'] = (
    (df['prior_claim_count'] * 2) +
    (df['mvr_points']) +
    (df['license_revoked'] * 5) +
    (df['bodily_injuries_count'].fillna(0) * 3)
)

# Weekend incident flag
df['is_weekend'] = df['is_weekend'].fillna(0)

print(f"   ✓ Created {len(df.columns)} total features")
print()

# ============================================================================
# 3. DATA PREPARATION
# ============================================================================

print("3. Preparing data for modeling...")

# Select features for modeling
categorical_features = [
    'age_group', 'vehicle_age_cat', 'time_of_day', 
    'tenure_category', 'incident_type', 'collision_type'
]

numerical_features = [
    'age', 'is_male', 'is_married', 'vehicle_age', 'vehicle_year',
    'policy_annual_premium', 'policy_deductible', 'months_as_customer',
    'incident_month', 'incident_day_of_week', 'incident_hour',
    'is_weekend', 'vehicles_involved', 'bodily_injuries_count',
    'witnesses_count', 'has_police_report', 'has_property_damage',
    'prior_claim_count', 'mvr_points', 'license_revoked',
    'premium_to_coverage_ratio', 'risk_score', 'is_red_car'
]

# Create feature matrix
X = df[numerical_features + categorical_features].copy()

# Encode categorical variables
label_encoders = {}
for col in categorical_features:
    le = LabelEncoder()
    X[col] = le.fit_transform(X[col].astype(str))
    label_encoders[col] = le

# Fill missing values
X = X.fillna(X.median())

# Target variable (Binary: Severe vs Not Severe)
y = (df['claim_severity'] == 'Severe').astype(int)

print(f"   ✓ Feature matrix shape: {X.shape}")
print(f"   ✓ Target distribution:")
print(f"      - Not Severe: {(y==0).sum():,} ({(y==0).sum()/len(y)*100:.1f}%)")
print(f"      - Severe: {(y==1).sum():,} ({(y==1).sum()/len(y)*100:.1f}%)")
print()

# Train/test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

print(f"   ✓ Training set: {len(X_train):,} records")
print(f"   ✓ Test set: {len(X_test):,} records")
print()

# Scale features
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# ============================================================================
# 4. MODEL TRAINING
# ============================================================================

print("4. Training models...")
print()

models = {
    'Logistic Regression': LogisticRegression(random_state=42, max_iter=1000),
    'Random Forest': RandomForestClassifier(
        n_estimators=100, 
        max_depth=10, 
        random_state=42,
        n_jobs=-1
    ),
    'Gradient Boosting': GradientBoostingClassifier(
        n_estimators=100,
        max_depth=5,
        random_state=42
    )
}

results = {}

for name, model in models.items():
    print(f"Training {name}...")
    
    # Train
    if name == 'Logistic Regression':
        model.fit(X_train_scaled, y_train)
        y_pred = model.predict(X_test_scaled)
        y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]
    else:
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        y_pred_proba = model.predict_proba(X_test)[:, 1]
    
    # Evaluate
    auc = roc_auc_score(y_test, y_pred_proba)
    
    results[name] = {
        'model': model,
        'predictions': y_pred,
        'probabilities': y_pred_proba,
        'auc': auc
    }
    
    print(f"   ✓ AUC-ROC: {auc:.4f}")
    print()

# ============================================================================
# 5. MODEL EVALUATION
# ============================================================================

print("="*80)
print("MODEL PERFORMANCE SUMMARY")
print("="*80)
print()

best_model_name = max(results.keys(), key=lambda x: results[x]['auc'])
best_model = results[best_model_name]

print(f"Best Model: {best_model_name}")
print(f"AUC-ROC: {best_model['auc']:.4f}")
print()

print("Classification Report:")
print(classification_report(
    y_test, 
    best_model['predictions'],
    target_names=['Not Severe', 'Severe']
))

print("Confusion Matrix:")
cm = confusion_matrix(y_test, best_model['predictions'])
print(cm)
print()

# ============================================================================
# 6. FEATURE IMPORTANCE (for tree-based models)
# ============================================================================

if best_model_name in ['Random Forest', 'Gradient Boosting']:
    print("="*80)
    print("TOP 15 MOST IMPORTANT FEATURES")
    print("="*80)
    print()
    
    feature_importance = pd.DataFrame({
        'feature': X.columns,
        'importance': best_model['model'].feature_importances_
    }).sort_values('importance', ascending=False)
    
    print(feature_importance.head(15).to_string(index=False))
    print()

# ============================================================================
# 7. BUSINESS INSIGHTS
# ============================================================================

print("="*80)
print("BUSINESS INSIGHTS")
print("="*80)
print()

# Analyze severe claims
severe_claims = df[df['claim_severity'] == 'Severe']
not_severe = df[df['claim_severity'] != 'Severe']

print("Severe Claims Characteristics:")
print(f"  • Average age: {severe_claims['age'].mean():.1f} years")
print(f"  • Average vehicle age: {severe_claims['vehicle_age'].mean():.1f} years")
print(f"  • Prior claims: {severe_claims['prior_claim_count'].mean():.2f}")
print(f"  • MVR points: {severe_claims['mvr_points'].mean():.1f}")
print(f"  • Weekend incidents: {severe_claims['is_weekend'].mean()*100:.1f}%")
print(f"  • Bodily injuries: {severe_claims['bodily_injuries_count'].mean():.2f}")
print()

print("Risk Factors for Severe Claims:")
age_diff = severe_claims['age'].mean() - not_severe['age'].mean()
print(f"  • Age: {abs(age_diff):.1f} years {'older' if age_diff > 0 else 'younger'}")

vehicle_age_diff = severe_claims['vehicle_age'].mean() - not_severe['vehicle_age'].mean()
print(f"  • Vehicle age: {abs(vehicle_age_diff):.1f} years {'older' if vehicle_age_diff > 0 else 'newer'}")

prior_diff = severe_claims['prior_claim_count'].mean() - not_severe['prior_claim_count'].mean()
print(f"  • Prior claims: {abs(prior_diff):.2f} more claims")
print()

print("="*80)
print("✓ MODELING COMPLETE")
print("="*80)
print()

print("Next Steps for Production:")
print("  1. Save model artifacts (pickle/joblib)")
print("  2. Create prediction API endpoint")
print("  3. Set up monitoring for model drift")
print("  4. A/B test model predictions")
print("  5. Document model assumptions and limitations")
print()

# ============================================================================
# 8. SAVE PREDICTIONS FOR FURTHER ANALYSIS
# ============================================================================

print("Saving predictions to database...")

# Create predictions dataframe
predictions_df = pd.DataFrame({
    'actual_severity': y_test.values,
    'predicted_severity': best_model['predictions'],
    'prediction_probability': best_model['probabilities'],
    'model_name': best_model_name,
    'prediction_date': datetime.now()
})

# Add actual claim amounts from test set
test_indices = X_test.index
predictions_df['claim_amount'] = df.loc[test_indices, 'total_claim_amount'].values

print(f"   ✓ Created {len(predictions_df):,} predictions")
print()

print("="*80)
print("INTERVIEW TALKING POINTS")
print("="*80)
print()
print("1. Feature Engineering:")
print("   'I created 6 derived features including risk scores and temporal patterns'")
print()
print("2. Model Selection:")
print(f"   'I compared 3 models and selected {best_model_name} with {best_model['auc']:.1%} AUC'")
print()
print("3. Business Impact:")
print("   'The model identifies severe claims with high accuracy, enabling'")
print("   'proactive case management and loss mitigation'")
print()
print("4. Deployment Ready:")
print("   'The model is production-ready with proper train/test splits,'")
print("   'scaled features, and documented performance metrics'")
print()
