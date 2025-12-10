# ===========================
# validate.py
# ===========================
# Purpose: Validate loaded Telco Churn data (local + Supabase)

import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv


# ------------------------------------------------------
# Supabase client helpers
# ------------------------------------------------------
def get_supabase_client() -> Client:
    """Initialize and return Supabase client."""
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env")

    return create_client(url, key)


def get_supabase_row_count(table_name: str = "telco_churn") -> int:
    """Return exact row count from Supabase table."""
    supabase = get_supabase_client()
    # count="exact" to get full row count
    response = supabase.table(table_name).select("id", count="exact").limit(1).execute()
    # supabase-py v2: count is on the response
    if hasattr(response, "count") and response.count is not None:
        return response.count
    # fallback
    return len(response.data)


# ------------------------------------------------------
# Main validation
# ------------------------------------------------------
def validate():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    raw_path = os.path.join(base_dir, "data", "raw", "churn_raw.csv")
    staged_path = os.path.join(base_dir, "data", "staged", "churn_transformed.csv")

    if not os.path.exists(raw_path):
        print(f"❌ Raw file not found at: {raw_path}")
        return

    if not os.path.exists(staged_path):
        print(f"❌ Transformed file not found at: {staged_path}")
        print("ℹ️  Please run transform.py first.")
        return

    raw_df = pd.read_csv(raw_path)
    df = pd.read_csv(staged_path)

    print("🔍 Starting validation...\n")

    checks = []

    # 1) No missing values in tenure, MonthlyCharges, TotalCharges
    numeric_cols = ["tenure", "MonthlyCharges", "TotalCharges"]
    missing_numeric = {col: int(df[col].isna().sum()) for col in numeric_cols if col in df.columns}
    no_missing_numeric = all(count == 0 for count in missing_numeric.values())
    checks.append(("No missing values in tenure, MonthlyCharges, TotalCharges", no_missing_numeric))

    if no_missing_numeric:
        print("✅ Check 1: No missing values in tenure, MonthlyCharges, TotalCharges")
    else:
        print("❌ Check 1: Missing values detected in numeric columns")

    # 2) Unique count of rows = original dataset
    if "customerID" in raw_df.columns:
        raw_unique = raw_df["customerID"].nunique()
    else:
        raw_unique = len(raw_df)

    transformed_rows = len(df)
    unique_match = (transformed_rows == raw_unique)
    checks.append(("Unique count of rows matches original dataset", unique_match))

    if unique_match:
        print(f"✅ Check 2: Unique/original rows match ({raw_unique} rows)")
    else:
        print(f"❌ Check 2: Row mismatch – original unique: {raw_unique}, transformed: {transformed_rows}")

    # 3) Row count matches Supabase table
    supabase_ok = False
    supabase_count = None
    try:
        supabase_count = get_supabase_row_count("telco_churn")
        supabase_ok = (supabase_count == transformed_rows)
    except Exception as e:
        print(f"❌ Could not get Supabase row count: {e}")
        supabase_ok = False

    checks.append(("Row count matches Supabase table", supabase_ok))

    if supabase_ok:
        print(f"✅ Check 3: Supabase row count matches transformed data ({supabase_count} rows)")
    else:
        if supabase_count is not None:
            print(f"❌ Check 3: Supabase row count ({supabase_count}) != transformed rows ({transformed_rows})")
        else:
            print("❌ Check 3: Failed to validate row count against Supabase")

    # 4) All tenure_group segments exist
    expected_tenure_groups = {"New", "Regular", "Loyal", "Champion"}
    if "tenure_group" in df.columns:
        actual_tenure_groups = set(df["tenure_group"].dropna().astype(str).unique())
    else:
        actual_tenure_groups = set()

    tenure_segments_ok = expected_tenure_groups.issubset(actual_tenure_groups)
    checks.append(("All tenure_group segments exist", tenure_segments_ok))

    if tenure_segments_ok:
        print(f"✅ Check 4: All tenure_group segments exist: {expected_tenure_groups}")
    else:
        print(f"❌ Check 4: Missing tenure_group segments. Found: {actual_tenure_groups}")

    # 5) All monthly_charge_segment segments exist
    expected_charge_segments = {"Low", "Medium", "High"}
    if "monthly_charge_segment" in df.columns:
        actual_charge_segments = set(df["monthly_charge_segment"].dropna().astype(str).unique())
    else:
        actual_charge_segments = set()

    charge_segments_ok = expected_charge_segments.issubset(actual_charge_segments)
    checks.append(("All monthly_charge_segment segments exist", charge_segments_ok))

    if charge_segments_ok:
        print(f"✅ Check 5: All monthly_charge_segment segments exist: {expected_charge_segments}")
    else:
        print(f"❌ Check 5: Missing monthly_charge_segment segments. Found: {actual_charge_segments}")

    # 6) Contract codes only {0,1,2}
    allowed_codes = {0, 1, 2}
    if "contract_type_code" in df.columns:
        actual_codes = set(df["contract_type_code"].dropna().unique())
    else:
        actual_codes = set()

    contract_codes_ok = actual_codes.issubset(allowed_codes) and len(actual_codes) > 0
    checks.append(("Contract codes are only {0,1,2}", contract_codes_ok))

    if contract_codes_ok:
        print(f"✅ Check 6: contract_type_code values valid: {actual_codes}")
    else:
        print(f"❌ Check 6: Invalid contract_type_code values found: {actual_codes}")

    # -------- Summary --------
    print("\n============================")
    print("VALIDATION SUMMARY")
    print("============================")
    all_ok = True
    for name, ok in checks:
        status = "PASS" if ok else "FAIL"
        print(f"- {name}: {status}")
        if not ok:
            all_ok = False

    if all_ok:
        print("\n🎉 All validation checks passed. Dataset is consistent and ready for ML.")
    else:
        print("\n⚠️ Some validation checks failed. Please review the issues above.")


if __name__ == "__main__":
    validate()
