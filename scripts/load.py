# ===========================
# load.py
# ===========================
# Purpose: Load transformed Telco Churn dataset into Supabase using Supabase client

import os
import time
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

BATCH_SIZE = 200
MAX_RETRIES = 3

# ------------------------------------------------------
# Supabase client
# ------------------------------------------------------
def get_supabase_client() -> Client:
    """Initialize and return Supabase client."""
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
   
    if not url or not key:
        raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env")
       
    return create_client(url, key)
 
# ------------------------------------------------------
# Step 1: Create table if not exists
# ------------------------------------------------------
def create_table_if_not_exists(table_name: str = "telco_churn"):
    """
    Ensures the telco_churn table exists in Supabase.
    """
    try:
        supabase = get_supabase_client()
       
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS public.telco_churn (
            id BIGSERIAL PRIMARY KEY,
            tenure INTEGER,
            "MonthlyCharges" DOUBLE PRECISION,
            "TotalCharges" DOUBLE PRECISION,
            "Churn" TEXT,
            "InternetService" TEXT,
            "Contract" TEXT,
            "PaymentMethod" TEXT,
            tenure_group TEXT,
            monthly_charge_segment TEXT,
            has_internet_service INTEGER,
            is_multi_line_user INTEGER,
            contract_type_code INTEGER
        );
        """
       
        try:
            # This assumes you have an RPC function execute_sql in your DB
            supabase.rpc('execute_sql', {'query': create_table_sql}).execute()
            print(f"✅ Table '{table_name}' created or already exists")
        except Exception as e:
            print(f"ℹ️  Note while creating table: {e}")
            print("ℹ️  If RPC 'execute_sql' is not available, create table manually in SQL editor.")
 
    except Exception as e:
        print(f"⚠️  Error checking/creating table: {e}")
        print("ℹ️  Trying to continue with data insertion...")
 
# ------------------------------------------------------
# Step 2: Load CSV data into Supabase table
# ------------------------------------------------------
def load_to_supabase(staged_path: str, table_name: str = "telco_churn"):
    """
    Load a transformed CSV into a Supabase table.

    Args:
        staged_path (str): Path to the transformed CSV file.
        table_name (str): Supabase table name. Default is 'telco_churn'.
    """
    # Convert to absolute path
    if not os.path.isabs(staged_path):
        staged_path = os.path.abspath(os.path.join(os.path.dirname(__file__), staged_path))
   
    print(f"🔍 Looking for data file at: {staged_path}")
   
    if not os.path.exists(staged_path):
        print(f"❌ Error: File not found at {staged_path}")
        print("ℹ️  Please run transform.py first to generate the transformed data")
        return
 
    try:
        supabase = get_supabase_client()
       
        # Read full CSV
        df = pd.read_csv(staged_path)

        # Keep only columns that match table schema
        required_cols = [
            "tenure",
            "MonthlyCharges",
            "TotalCharges",
            "Churn",
            "InternetService",
            "Contract",
            "PaymentMethod",
            "tenure_group",
            "monthly_charge_segment",
            "has_internet_service",
            "is_multi_line_user",
            "contract_type_code",
        ]

        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            print(f"❌ Missing required columns in CSV: {missing}")
            return

        df = df[required_cols]

        total_rows = len(df)
        print(f"📊 Loading {total_rows} rows into '{table_name}'...")

        # Process in batches
        for i in range(0, total_rows, BATCH_SIZE):
            batch = df.iloc[i:i + BATCH_SIZE].copy()

            # Convert NaN to None for proper NULL handling
            batch = batch.where(pd.notnull(batch), None)
            records = batch.to_dict('records')

            batch_number = i // BATCH_SIZE + 1
            attempt = 1

            while attempt <= MAX_RETRIES:
                try:
                    response = supabase.table(table_name).insert(records).execute()
                    # For new supabase-py, errors typically raise exceptions,
                    # but we keep this extra check just in case:
                    if hasattr(response, "error") and response.error:
                        raise Exception(response.error)
                   
                    end = min(i + BATCH_SIZE, total_rows)
                    print(f"✅ Batch {batch_number}: Inserted rows {i+1}-{end} of {total_rows}")
                    break  # success → exit retry loop

                except Exception as e:
                    print(f"⚠️  Error in batch {batch_number}, attempt {attempt}: {str(e)}")
                    if attempt == MAX_RETRIES:
                        print(f"❌ Max retries reached for batch {batch_number}. Skipping this batch.")
                        break
                    wait_time = 2 * attempt
                    print(f"⏳ Retrying batch {batch_number} in {wait_time} seconds...")
                    time.sleep(wait_time)
                    attempt += 1
 
        print(f"🎯 Finished loading data into '{table_name}'.")
 
    except Exception as e:
        print(f"❌ Error loading data: {e}")
 
# ------------------------------------------------------
# Step 3: Run as standalone script
# ------------------------------------------------------
if __name__ == "__main__":
    # Path relative to the script location
    staged_csv_path = os.path.join("..", "data", "staged", "churn_transformed.csv")
    create_table_if_not_exists()
    load_to_supabase(staged_csv_path)
