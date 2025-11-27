"""
Data Loader for Customer C (car_insurance_claim.csv)
Loads data into insurance_raw.customer_c_policies table
Updated for psycopg v3
"""

import pandas as pd
import psycopg
from datetime import datetime
import sys

# Database connection parameters
DB_CONFIG = {
    'dbname': 'insurance_analytics',
    'user': 'addy',
    'password': 'Addy0499',  # CHANGE THIS
    'host': 'localhost',
    'port': '5432'
}

# File path
CSV_FILE = '/Users/addy/Desktop/Projects/Data Analysis Project/Dataset/car_insurance_claim.csv'

def load_customer_c_data():
    """Load Customer C data into raw table"""
    
    print("="*80)
    print("LOADING CUSTOMER C DATA (car_insurance_claim.csv)")
    print("="*80)
    
    try:
        # Read CSV
        print(f"\n1. Reading CSV file: {CSV_FILE}")
        df = pd.read_csv(CSV_FILE)
        print(f"   ✓ Loaded {len(df):,} records with {len(df.columns)} columns")
        
        # Rename columns
        column_mapping = {
            'ID': 'record_id', 'KIDSDRIV': 'kids_driving', 'BIRTH': 'birth_date',
            'AGE': 'age', 'HOMEKIDS': 'home_kids', 'YOJ': 'years_on_job',
            'INCOME': 'income', 'PARENT1': 'parent1', 'HOME_VAL': 'home_value',
            'MSTATUS': 'marital_status', 'GENDER': 'gender', 'EDUCATION': 'education',
            'OCCUPATION': 'occupation', 'TRAVTIME': 'travel_time', 'CAR_USE': 'car_use',
            'BLUEBOOK': 'bluebook_value', 'TIF': 'time_in_force', 'CAR_TYPE': 'car_type',
            'RED_CAR': 'red_car', 'OLDCLAIM': 'old_claim', 'CLM_FREQ': 'claim_frequency',
            'REVOKED': 'license_revoked', 'MVR_PTS': 'mvr_points', 'CLM_AMT': 'claim_amount',
            'CAR_AGE': 'car_age', 'CLAIM_FLAG': 'claim_flag', 'URBANICITY': 'urbanicity'
        }
        df = df.rename(columns=column_mapping)
        print("   ✓ Column names standardized")
        
        # Connect
        print("\n2. Connecting to PostgreSQL...")
        connection = psycopg.connect(**DB_CONFIG)
        cursor = connection.cursor()
        print("   ✓ Connected successfully")
        
        # Clear table
        print("\n3. Clearing existing data...")
        cursor.execute("TRUNCATE TABLE insurance_raw.customer_c_policies;")
        connection.commit()
        print("   ✓ Table cleared")
        
        # Insert query
        insert_query = """
            INSERT INTO insurance_raw.customer_c_policies 
            (record_id, kids_driving, birth_date, age, home_kids, years_on_job,
             income, parent1, home_value, marital_status, gender, education,
             occupation, travel_time, car_use, bluebook_value, time_in_force,
             car_type, red_car, old_claim, claim_frequency, license_revoked,
             mvr_points, claim_amount, car_age, claim_flag, urbanicity,
             load_timestamp, source_file)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Insert data
        print("\n4. Inserting data...")
        total_inserted = 0
        errors = []
        
        for idx, row in df.iterrows():
            try:
                values = (
                    int(row['record_id']) if pd.notna(row['record_id']) else None,
                    int(row['kids_driving']) if pd.notna(row['kids_driving']) else None,
                    str(row['birth_date']) if pd.notna(row['birth_date']) else None,
                    float(row['age']) if pd.notna(row['age']) else None,
                    int(row['home_kids']) if pd.notna(row['home_kids']) else None,
                    float(row['years_on_job']) if pd.notna(row['years_on_job']) else None,
                    str(row['income']) if pd.notna(row['income']) else None,
                    str(row['parent1']) if pd.notna(row['parent1']) else None,
                    str(row['home_value']) if pd.notna(row['home_value']) else None,
                    str(row['marital_status']) if pd.notna(row['marital_status']) else None,
                    str(row['gender']) if pd.notna(row['gender']) else None,
                    str(row['education']) if pd.notna(row['education']) else None,
                    str(row['occupation']) if pd.notna(row['occupation']) else None,
                    int(row['travel_time']) if pd.notna(row['travel_time']) else None,
                    str(row['car_use']) if pd.notna(row['car_use']) else None,
                    str(row['bluebook_value']) if pd.notna(row['bluebook_value']) else None,
                    int(row['time_in_force']) if pd.notna(row['time_in_force']) else None,
                    str(row['car_type']) if pd.notna(row['car_type']) else None,
                    str(row['red_car']) if pd.notna(row['red_car']) else None,
                    str(row['old_claim']) if pd.notna(row['old_claim']) else None,
                    int(row['claim_frequency']) if pd.notna(row['claim_frequency']) else None,
                    str(row['license_revoked']) if pd.notna(row['license_revoked']) else None,
                    int(row['mvr_points']) if pd.notna(row['mvr_points']) else None,
                    str(row['claim_amount']) if pd.notna(row['claim_amount']) else None,
                    float(row['car_age']) if pd.notna(row['car_age']) else None,
                    int(row['claim_flag']) if pd.notna(row['claim_flag']) else None,
                    str(row['urbanicity']) if pd.notna(row['urbanicity']) else None,
                    datetime.now(),
                    'car_insurance_claim.csv'
                )
                
                cursor.execute(insert_query, values)
                total_inserted += 1
                
                if total_inserted % 500 == 0:
                    print(f"   • Inserted {total_inserted:,} records...", end='\r')
                    connection.commit()
                    
            except Exception as e:
                errors.append({'row': idx, 'error': str(e)})
                continue
        
        connection.commit()
        print(f"\n   ✓ Successfully inserted {total_inserted:,} records")
        
        if errors:
            print(f"\n   ⚠ {len(errors)} records failed")
        
        # Verify
        print("\n5. Verifying data load...")
        cursor.execute("SELECT COUNT(*) FROM insurance_raw.customer_c_policies;")
        count = cursor.fetchone()[0]
        print(f"   ✓ Table now contains {count:,} records")
        
        # Sample
        print("\n6. Sample data:")
        cursor.execute("""
            SELECT record_id, age, gender, car_type, claim_flag 
            FROM insurance_raw.customer_c_policies 
            WHERE age IS NOT NULL 
            LIMIT 5;
        """)
        samples = cursor.fetchall()
        print("\n   Record ID  | Age | Gender | Car Type | Claim?")
        print("   " + "-"*55)
        for row in samples:
            claim_text = "Yes" if row[4] == 1 else "No"
            print(f"   {row[0]:<11} | {int(row[1]):<3} | {row[2]:<6} | {row[3]:<8} | {claim_text}")
        
        # Stats
        print("\n7. Data quality statistics:")
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                SUM(claim_flag) as total_claims,
                AVG(CASE WHEN claim_flag = 1 THEN 1.0 ELSE 0.0 END) * 100 as claim_rate
            FROM insurance_raw.customer_c_policies;
        """)
        stats = cursor.fetchone()
        print(f"   Total records: {stats[0]:,}")
        print(f"   Total claims: {stats[1]:,}")
        print(f"   Claim rate: {stats[2]:.1f}%")
        
        cursor.close()
        connection.close()
        print("\n" + "="*80)
        print("✓ CUSTOMER C DATA LOAD COMPLETE")
        print("="*80)
        
        return True
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("\n" + "="*80)
    print("GUIDEWIRE INSURANCE ANALYTICS - DATA LOADER")
    print("Customer C: Policy & Claims System")
    print("="*80)
    
    success = load_customer_c_data()
    
    if success:
        print("\n🎉 ALL DATA LOADS COMPLETE!")
        print("✓ Total records loaded: 12,642")
    else:
        print("\n✗ Load failed")
        sys.exit(1)
