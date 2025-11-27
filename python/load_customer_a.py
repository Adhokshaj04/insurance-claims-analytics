"""
Data Loader for Customer A (insurance_claims.csv)
Loads data into insurance_raw.customer_a_claims table
Updated for psycopg v3
"""

import pandas as pd
import psycopg
from datetime import datetime
import sys

# Database connection parameters
DB_CONFIG = {
    'dbname': 'insurance_analytics',
    'user': 'addy',  # Your Mac username
    'password': 'Addy0499',  # CHANGE THIS to the password you set
    'host': 'localhost',
    'port': '5432'
}

# File path - your CSV file location
CSV_FILE = '/Users/addy/Desktop/Projects/Data Analysis Project/Dataset/insurance_claims.csv'

def clean_column_name(col):
    """Clean column names for SQL compatibility"""
    return col.lower().replace('-', '_').replace(' ', '_')

def load_customer_a_data():
    """Load Customer A data into raw table"""
    
    print("="*80)
    print("LOADING CUSTOMER A DATA (insurance_claims.csv)")
    print("="*80)
    
    try:
        # Read CSV file
        print(f"\n1. Reading CSV file: {CSV_FILE}")
        df = pd.read_csv(CSV_FILE)
        print(f"   ✓ Loaded {len(df):,} records with {len(df.columns)} columns")
        
        # Clean column names
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Remove the problematic _c39 column (all nulls)
        if '_c39' in df.columns:
            df = df.drop('_c39', axis=1)
            print("   ✓ Removed empty _c39 column")
        
        # Connect to database
        print("\n2. Connecting to PostgreSQL database...")
        connection = psycopg.connect(**DB_CONFIG)
        cursor = connection.cursor()
        print("   ✓ Connected successfully")
        
        # Clear existing data
        print("\n3. Clearing existing data from table...")
        cursor.execute("TRUNCATE TABLE insurance_raw.customer_a_claims;")
        connection.commit()
        print("   ✓ Table cleared")
        
        # Prepare insert statement
        columns = df.columns.tolist()
        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"""
            INSERT INTO insurance_raw.customer_a_claims 
            ({', '.join(columns)}, load_timestamp, source_file)
            VALUES ({placeholders}, %s, %s)
        """
        
        # Insert data in batches
        print("\n4. Inserting data...")
        batch_size = 100
        total_inserted = 0
        errors = []
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            
            for idx, row in batch.iterrows():
                try:
                    # Convert row to list and add metadata
                    values = row.tolist()
                    values.append(datetime.now())  # load_timestamp
                    values.append('insurance_claims.csv')  # source_file
                    
                    cursor.execute(insert_query, values)
                    total_inserted += 1
                    
                    if total_inserted % 100 == 0:
                        print(f"   • Inserted {total_inserted:,} records...", end='\r')
                        
                except Exception as e:
                    errors.append({
                        'row': idx,
                        'error': str(e),
                        'data': row.to_dict()
                    })
                    continue
            
            connection.commit()
        
        print(f"\n   ✓ Successfully inserted {total_inserted:,} records")
        
        # Report errors if any
        if errors:
            print(f"\n   ⚠ {len(errors)} records failed to insert")
            print("\n   First 5 errors:")
            for i, error in enumerate(errors[:5], 1):
                print(f"     {i}. Row {error['row']}: {error['error']}")
        
        # Verify data
        print("\n5. Verifying data load...")
        cursor.execute("SELECT COUNT(*) FROM insurance_raw.customer_a_claims;")
        count = cursor.fetchone()[0]
        print(f"   ✓ Table now contains {count:,} records")
        
        # Show sample data
        print("\n6. Sample data from table:")
        cursor.execute("""
            SELECT policy_number, age, incident_type, total_claim_amount 
            FROM insurance_raw.customer_a_claims 
            LIMIT 5;
        """)
        samples = cursor.fetchall()
        print("\n   Policy#  | Age | Incident Type           | Claim Amount")
        print("   " + "-"*65)
        for row in samples:
            print(f"   {row[0]:<9} | {row[1]:<3} | {row[2]:<23} | ${row[3]:>11,}")
        
        # Close connection
        cursor.close()
        connection.close()
        print("\n" + "="*80)
        print("✓ CUSTOMER A DATA LOAD COMPLETE")
        print("="*80)
        
        return True
        
    except FileNotFoundError:
        print(f"\n✗ ERROR: File not found: {CSV_FILE}")
        print("  Please check the file path.")
        return False
        
    except psycopg.Error as e:
        print(f"\n✗ DATABASE ERROR: {e}")
        return False
        
    except Exception as e:
        print(f"\n✗ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("\n" + "="*80)
    print("GUIDEWIRE INSURANCE ANALYTICS - DATA LOADER")
    print("Customer A: Comprehensive Claims System")
    print("="*80)
    
    # Check if user updated password
    if DB_CONFIG['password'] == 'password123':
        print("\n⚠ WARNING: Please update DB_CONFIG password!")
        print("  Edit this file and change 'password123' to your actual password")
        response = input("\nDo you want to continue anyway? (yes/no): ")
        if response.lower() != 'yes':
            sys.exit(1)
    
    # Run the loader
    success = load_customer_a_data()
    
    if success:
        print("\n✓ Next step: Run load_customer_b.py to load AutoBi.csv")
    else:
        print("\n✗ Load failed. Please fix errors and try again.")
        sys.exit(1)
