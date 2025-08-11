"""
This script performs a comprehensive customer-level analysis of insurance data.
It loads and merges data from four separate CSV files, then engineers a range
of features to calculate each customer's lifetime value (LTV), loss ratio,
and a composite risk score.

Based on these metrics, customers are classified into four distinct segments
('Premium Partner', 'Growth Prospect', 'Risk Management', 'Watch List') to
enable targeted business actions. The final output is a CSV report and a
console summary highlighting key insights.
"""


import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.preprocessing import MinMaxScaler

# data loading function
#Loads all necessary CSV files into pandas DataFrames.
def load_data(claims_path, policies_path, customers_path, fraud_path):
    """Loads all necessary CSV files into pandas DataFrames."""

    try:
        claims_df = pd.read_csv(claims_path)
        policies_df = pd.read_csv(policies_path)
        customers_df = pd.read_csv(customers_path)
        fraud_df = pd.read_csv(fraud_path)
        return claims_df, policies_df, customers_df, fraud_df
    except FileNotFoundError as e:
        print(f"File not found: {e}")
        exit()


# Merges the four raw dataframes into a single master dataframe.
def merge_dataframes(claims_df, policies_df, customers_df, fraud_df):
    """Merges the four raw dataframes into a single master dataframe.
    The merge strategy uses left joins starting from the customers table to
    ensure all customers are retained in the final master table, even if they
    have no associated policies or claims."""
    claims_with_fraud = pd.merge(claims_df, fraud_df, on='claim_id', how='left')
    policies_with_claims = pd.merge(policies_df, claims_with_fraud, on='policy_id', how='left')
    master_df = pd.merge(customers_df, policies_with_claims, on='customer_id', how='left')

    for col in master_df.columns:
        if 'date' in col.lower():
            master_df[col] = pd.to_datetime(master_df[col], errors='coerce')
    
    print("Dataframes merged successfully.")
    return master_df


# Calculates customer-level features from the master dataframe.
def calculate_customer_features(master_df, policies_df):
    """ Aggregates the master dataframe to a customer-level analytical table.
    This function groups the data by customer to create key features such as
    policy tenure, total claims, and total premium from active policies."""
    # Group the master table by each customer to calculate their individual metrics.
    customer_analysis = master_df.groupby('customer_id').agg(
        # Find the earliest policy start date to determine tenure.
        first_policy_start=('start_date', 'min'),
        # Count the number of unique claims for each customer.
        total_claims=('claim_id', 'nunique'),
        # Sum the total dollar amount of all claims made by the customer.
        total_claim_amount=('claim_amount', 'sum'),
        # Count fraudulent claims; fill NaNs with False and sum True values.
        fraud_claims=('is_fraudulent', lambda x: x.fillna(False).sum())
    ).reset_index()

    # Calculate policy tenure in days: current date minus their first policy start date.
    customer_analysis['policy_tenure_days'] = (datetime.now() - customer_analysis['first_policy_start']).dt.days
    # Convert tenure to years, using 365.25 to account for leap years.
    customer_analysis['policy_tenure_years'] = customer_analysis['policy_tenure_days'] / 365.25

    # From the policies table, filter for only 'ACTIVE' policies.
    active_premiums = policies_df[policies_df['status'] == 'ACTIVE'].groupby('customer_id')['annual_premium'].sum().reset_index()
    # Rename the column for clarity before merging.
    active_premiums = active_premiums.rename(columns={'annual_premium': 'annual_premium_sum'})

    # Merge the sum of active premiums into the customer analysis table.
    # A 'left' merge ensures all customers are kept, even if they have no active policies.
    customer_analysis = pd.merge(customer_analysis, active_premiums, on='customer_id', how='left')
    # For customers with no active policies, fill the resulting NaN with 0.
    customer_analysis['annual_premium_sum'] = customer_analysis['annual_premium_sum'].fillna(0)
    return customer_analysis


# Lifetime Value (LTV) and Loss Ratio Calculation
def calculate_ltv_and_loss_ratio(df):
    """Calculates customer lifetime value (LTV) and loss ratio."""
    df['lifetime_value'] = df['annual_premium_sum'] - df['total_claim_amount']
    df['loss_ratio'] = (df['total_claim_amount'] / df['annual_premium_sum']).replace([np.inf, -np.inf], 0).fillna(0)
    return df


# Risk Score Calculation
def calculate_risk_score(df):
    """Calculates a normalized, weighted risk score for each customer.
    This function first calculates claim frequency, then normalizes all risk
    features to a 0-1 scale using MinMaxScaler. Finally, it computes a
    composite score by applying weights to the scaled features."""
    df['claim_frequency_per_year'] = (df['total_claims'] / df['policy_tenure_years']).replace([np.inf, -np.inf], 0).fillna(0)
    
    risk_features = ['loss_ratio', 'fraud_claims', 'claim_frequency_per_year']
    scaler = MinMaxScaler()
    scaled_features = scaler.fit_transform(df[risk_features])
    
    df['risk_score'] = (
        scaled_features[:, 0] * 50 +  # loss_ratio
        scaled_features[:, 1] * 30 +  # fraud_claims
        scaled_features[:, 2] * 20    # claim_frequency_per_year
    )
    return df

# Customer Segmentation
# Classifies each customer into a segment based on their LTV and risk score.
def segment_customers(df):
    """Classifies each customer into a segment based on their LTV and risk score.
    The segmentation rules are defined to create actionable business categories,
    from high-value partners to high-risk accounts needing review."""
    def assign_segment(row):
        if row['lifetime_value'] >= 0 and row['risk_score'] <= 40:
            return 'Premium Partner'
        elif row['lifetime_value'] >= 0 and 40 < row['risk_score'] <= 60:
            return 'Growth Prospect'
        elif row['lifetime_value'] < 0 and row['risk_score'] > 60:
            return 'Risk Management'
        else:
            return 'Watch List'

    df['segment'] = df.apply(assign_segment, axis=1)
    return df

# Final Deliverables
# Creates the final report and summary of the analysis.
def create_deliverables(df):
    """Creates the final CSV report and prints a summary to the console."""
    final_df = df[[
        'customer_id',
        'lifetime_value',
        'loss_ratio',
        'risk_score',
        'segment'
    ]].copy()
    
    output_filename = "customer_segmentation_report.csv"
    final_df.to_csv(output_filename, index=False)
    print(f"\nFinal report saved as '{output_filename}'")

    # E.2: Print a brief summary to the console
    segment_counts = final_df['segment'].value_counts()
    highest_risk_customers = final_df.sort_values(by='risk_score', ascending=False).head(3)

    print("\n--- Customer Analysis Summary ---")
    
    print("\nCustomer Distribution by Segment:")
    print(segment_counts.to_string())

    print("\nTop 3 Highest-Risk Customers:")
    print(highest_risk_customers.to_string(index=False))

    print("\nRecommended Next Step:")
    print("A manual review of the highest-risk customers' policies and claim history is "
          "strongly recommended for the underwriting and fraud teams. This will help "
          "validate the risk signals and determine appropriate actions.")

if __name__ == "__main__":

    CLAIMS_PATH = 'claims_sample.csv'
    POLICIES_PATH = 'policies_sample.csv'
    CUSTOMERS_PATH = 'customers_sample.csv'
    FRAUD_PATH = 'fraud_detection_sample.csv'

    claims, policies, customers, fraud = load_data(CLAIMS_PATH, POLICIES_PATH, CUSTOMERS_PATH, FRAUD_PATH)
    master_table = merge_dataframes(claims, policies, customers, fraud)
    customer_level_data = calculate_customer_features(master_table, policies)
    customer_level_data = calculate_ltv_and_loss_ratio(customer_level_data)
    customer_level_data = calculate_risk_score(customer_level_data)
    customer_level_data = segment_customers(customer_level_data)
    create_deliverables(customer_level_data)
