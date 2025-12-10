import os
import numpy as np
import pandas as pd


def transform_data():
    # Go up one level from scripts/ to project root
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    raw_dir = os.path.join(base_dir, "data", "raw")
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)

    raw_path = os.path.join(raw_dir, "churn_raw.csv")

    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"❌ Raw file not found at: {raw_path}")

    # ------------------- LOAD -------------------
    df = pd.read_csv(raw_path)

    # ------------------- CLEANING TASKS -------------------

    # 1) Convert TotalCharges to numeric (spaces -> NaN)
    df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors="coerce")

    # 2) Fill missing numeric values with median
    numeric_cols = ["tenure", "MonthlyCharges", "TotalCharges"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].median())

    # 3) Replace missing categorical values with "Unknown"
    cat_cols = df.select_dtypes(include=["object"]).columns
    df[cat_cols] = df[cat_cols].fillna("Unknown")

    # ------------------- FEATURE ENGINEERING -------------------

    # 1) tenure_group
    # 0–12   → "New"
    # 13–36  → "Regular"
    # 37–60  → "Loyal"
    # 60+    → "Champion"
    df["tenure_group"] = pd.cut(
        df["tenure"],
        bins=[-0.1, 12, 36, 60, float("inf")],
        labels=["New", "Regular", "Loyal", "Champion"],
        include_lowest=True,
        right=True
    )

    # 2) monthly_charge_segment
    # MonthlyCharges < 30  → "Low"
    # 30–70               → "Medium"
    # > 70                → "High"
    mc = df["MonthlyCharges"]
    conditions = [
        mc < 30,
        (mc >= 30) & (mc <= 70),
        mc > 70
    ]
    choices = ["Low", "Medium", "High"]
    df["monthly_charge_segment"] = np.select(conditions, choices, default="Unknown")

    # 3) has_internet_service
    # "DSL" / "Fiber optic" → 1, "No" → 0
    df["has_internet_service"] = df["InternetService"].map({
        "DSL": 1,
        "Fiber optic": 1,
        "No": 0
    }).fillna(0).astype(int)

    # 4) is_multi_line_user
    # 1 if MultipleLines == "Yes", else 0
    df["is_multi_line_user"] = (df["MultipleLines"] == "Yes").astype(int)

    # 5) contract_type_code
    # Month-to-month → 0, One year → 1, Two year → 2
    df["contract_type_code"] = df["Contract"].map({
        "Month-to-month": 0,
        "One year": 1,
        "Two year": 2
    }).astype("Int64")  # nullable integer

    # ------------------- DROP UNNECESSARY FIELDS -------------------
    df = df.drop(["customerID", "gender"], axis=1, errors="ignore")

    # ------------------- SAVE TO STAGED -------------------
    staged_path = os.path.join(staged_dir, "churn_transformed.csv")
    df.to_csv(staged_path, index=False)

    print(f"✅ Data transformed and saved at: {staged_path}")
    return staged_path


if __name__ == "__main__":
    transform_data()
