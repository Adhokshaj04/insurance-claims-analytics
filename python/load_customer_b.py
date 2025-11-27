"""
Data Loader for Customer B (AutoBi.csv)
Loads data into insurance_raw.customer_b_claims table
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
    'password': 'Addy0499',  # CHANGE THIS to your actual password
    'host': 'localhost',
    'port': '5432'
}

# File path
CSV_FILE = '/Users/addy/Desktop/Projects/Data Analysis Project/Dataset/AutoBi.csv'

def load_customer_b_data():
    """Load Customer B data into raw table"""
    
    print("="*80)
    print("LOADING CUSTOMER B DATA (AutoBi.csv)")
    print("="*80)
    
    try:
        # Read CSV file
        print(f"\n1. Reading CSV file: {CSV_FILE}")
        df = pd.read_csv(CSV_FILE)
        print(f"   ✓ Loaded {len(df):,} records with {len(df.columns)} columns")
        
        # Rename columns to match database schema
        column_mapping = {
            'Index': 'index_id',
            'CASENUM': 'case_number',
            'ATTORNEY': 'attorney',
            'CLMSEX': 'claimant_sex',
            'MARITAL': 'marital_status',
            'CLMINSUR': 'claimant_insured',
            'SEATBELT': 'seatbelt',
            'CLMAGE': 'claimant_age',
            'LOSS': 'loss_amount'
        }
        df = df.rename(columns=column_mapping)
        print("   ✓ Column names standardized")
        
        # Connect to database
        print("\n2. Connecting to PostgreSQL database...")
        connection = psycopg.connect(**DB_CONFIG)
        cursor = connection.cursor()
        print("   ✓ Connected successfully")
        
        # Clear existing data
        print("\n3. Clearing existing data from table...")
        cursor.execute("TRUNCATE TABLE insurance_raw.customer_b_claims;")
        connection.commit()
        print("   ✓ Table cleared")
        
        # Prepare insert statement
        insert_query = """
            INSERT INTO insurance_raw.customer_b_claims 
            (index_id, case_number, attorney, claimant_sex, marital_status, 
             claimant_insured, seatbelt, claimant_age, loss_amount, 
             load_timestamp, source_file)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Insert data
        print("\n4. Inserting data...")
        total_inserted = 0
        errors = []
        
        for idx, row in df.iterrows():
            try:
                values = (
                    int(row['index_id']) if pd.notna(row['index_id']) else None,
                    int(row['case_number']) if pd.notna(row['case_number']) else None,
                    int(row['attorney']) if pd.notna(row['attorney']) else None,
                    float(row['claimant_sex']) if pd.notna(row['claimant_sex']) else None,
                    float(row['marital_status']) if pd.notna(row['marital_status']) else None,
                    float(row['claimant_insured']) if pd.notna(row['claimant_insured']) else None,
                    float(row['seatbelt']) if pd.notna(row['seatbelt']) else None,
                    float(row['claimant_age']) if pd.notna(row['claimant_age']) else None,
                    float(row['loss_amount']) if pd.notna(row['loss_amount']) else None,
                    datetime.now(),
                    'AutoBi.csv'
                )
                
                cursor.execute(insert_query, values)
                total_inserted += 1
                
                if total_inserted % 100 == 0:
                    print(f"   • Inserted {total_inserted:,} records...", end='\r')
                    connection.commit()
                    
            except Exception as e:
                errors.append({'row': idx, 'error': str(e)})
                continue
        
        connection.commit()
        print(f"\n   ✓ Successfully inserted {total_inserted:,} records")
        
        if errors:
            print(f"\n   ⚠ {len(errors)} records failed")
        
        # Verify data
        print("\n5. Verifying data load...")
        cursor.execute("SELECT COUNT(*) FROM insurance_raw.customer_b_claims;")
        count = cursor.fetchone()[0]
        print(f"   ✓ Table now contains {count:,} records")
        
        # Show sample
        print("\n6. Sample data from table:")
        cursor.execute("""
            SELECT case_number, claimant_age, attorney, loss_amount 
            FROM insurance_raw.customer_b_claims 
            WHERE claimant_age IS NOT NULL 
            LIMIT 5;
        """)
        samples = cursor.fetchall()
        print("\n   Case#  | Age | Attorney | Loss Amount")
        print("   " + "-"*50)
        for row in samples:
            attorney_text = "Yes" if row[2] == 2 else "No"
            print(f"   {row[0]:<7} | {int(row[1]):<3} | {attorney_text:<8} | ${row[3]:>10,.2f}")
        
        cursor.close()
        connection.close()
        print("\n" + "="*80)
        print("✓ CUSTOMER B DATA LOAD COMPLETE")
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
    print("Customer B: Bodily Injury Claims System")
    print("="*80)
    
    success = load_customer_b_data()
    
    if success:
        print("\n✓ Next step: Run load_customer_c.py")
    else:
        print("\n✗ Load failed")
        sys.exit(1)
