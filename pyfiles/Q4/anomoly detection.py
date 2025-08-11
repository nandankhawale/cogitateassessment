import pandas as pd
import numpy as np
from datetime import datetime



#Loading data
# This function loads the data from CSV files and handles exceptions
def load_data():
    try:
        customers_df = pd.read_csv("customers_sample.csv")
        policies_df = pd.read_csv("policies_sample.csv")
        claims_df = pd.read_csv("claims_sample.csv")
    except Exception as e:
        raise RuntimeError(f"Error loading data: {e}")
    return customers_df, policies_df, claims_df



# Preprocessing data
# This function preprocesses the data by filling missing values and converting date columns
def preprocess_data(df):
    df = df.copy()
    df.fillna(0, inplace=True)
    for col in df.columns:
        if 'date' in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df



# Detecting outliers using IQR method
# This function detects outliers in the claim amounts based on the IQR method
def detect_outliers_iqr(df):
    df = df.copy()
    df["is_outlier"] = False
    for ptype in df["policy_type"].unique():
        subset = df[df["policy_type"] == ptype]["claim_amount"]
        q1 = subset.quantile(0.25)
        q3 = subset.quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        mask = (df["policy_type"] == ptype) & (
            (df["claim_amount"] < lower_bound) | (df["claim_amount"] > upper_bound)
        )
        df.loc[mask, "is_outlier"] = True
    return df



# Calculating risk scores based on various features
# This function calculates risk scores based on claim amount, policy coverage, and customer history
def calculate_risk_scores(df):
    df = df.copy()
    # risk score components
    # Claim amount to coverage ratio
    """
    Calculates a composite risk score for each claim using multiple factors.

    Current scoring system:
      1. Coverage Ratio Score (max 50 points)
         - Measures how large the claim is compared to the coverage amount.
         - Formula: coverage_ratio * 50 (capped at 50 points)

      2. Early Claim Score (max 30 points)
         - Flags if the claim occurred within 30 days of policy start.
         - If days_since_start < 30 → +30 points

      3. Claim History Score (max 20 points)
         - Adds points for multiple claims from the same customer.
         - +5 points per extra claim after the first, capped at 20 points

      4. Anomaly Reason Bonus
         - For each anomaly reason (e.g., multiple claims, high ratio, outlier),
           +5 points per reason.
         - This amplifies risk for multiple red flags.
"""
    df["coverage_ratio"] = df["claim_amount"] / df["coverage_amount"]
    df["score_coverage"] = df["coverage_ratio"].apply(lambda x: min(50, x * 50))

    # Days since policy started
    df["days_since_start"] = (df["claim_date"] - df["start_date"]).dt.days
    df["score_timing"] = df["days_since_start"].apply(lambda x: 30 if x < 30 else 0)

    # Customer claim history
    claim_counts = df.groupby("customer_id")["claim_id"].transform("count")
    df["score_history"] = ((claim_counts - 1).clip(lower=0) * 5).clip(upper=20)

    # Final risk score calculation
    df["risk_score"] = df["score_coverage"] + df["score_timing"] + df["score_history"]

    # Anomaly reasons
    # This section generates reasons for anomalies based on the risk scores and outliers
    reasons = []
    for _, row in df.iterrows():
        reason_list = []
        if row["score_history"] > 0:
            reason_list.append("Multiple claims history")
        if row["score_coverage"] > 40:
            reason_list.append("High claim-to-coverage ratio")
        if row["score_timing"] > 0:
            reason_list.append("Early claim after policy start")
        if row["is_outlier"]:
            reason_list.append("Statistical outlier in claim amount")
        reasons.append(", ".join(reason_list) if reason_list else "Normal")
    df["anomaly_reasons"] = reasons

    df["risk_score"] += df["anomaly_reasons"].apply(
        lambda r: (r.count(",") + 1) * 5 if r != "Normal" else 0
    )

    return df



def process_claims():
    customers_df, policies_df, claims_df = load_data()

    df = claims_df.merge(policies_df, on="policy_id", how="left")
    df = df.merge(customers_df, on="customer_id", how="left")

    df = preprocess_data(df)
    df = detect_outliers_iqr(df)
    df = calculate_risk_scores(df)

    
    output_df = df[["claim_id", "risk_score", "is_outlier", "anomaly_reasons"]].copy()

    
    output_df["reason_count"] = output_df["anomaly_reasons"].apply(lambda x: len(str(x).split(",")))
    output_df = output_df.sort_values(by=["reason_count", "risk_score"], ascending=[False, False])
    output_df = output_df.drop(columns=["reason_count"])

    output_df.to_csv("claims_anomaly_report.csv", index=False)
    print("\nTop 5 high-risk claims:\n", output_df.head())

if __name__ == "__main__":
    process_claims()


