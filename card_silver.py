import pandas as pd
import boto3

# Paths (S3/MinIO)
bronze_path = "s3://bronze/card_transactions.csv"
silver_path = "s3://silver/card_transactions.csv"

# Read CSV from MinIO/S3
# Requires: pip install pandas s3fs boto3
df_bronze = pd.read_csv(bronze_path)

# Transformations
df_silver = (
    df_bronze
    # keep only completed
    .loc[df_bronze["transaction_status"].str.lower() == "completed"]
    .copy()
)

# datetime conversion
df_silver["transaction_datetime"] = pd.to_datetime(
    df_silver["transaction_datetime"], format="%Y-%m-%d %H:%M:%S", errors="coerce"
)

# card_number mask (last 4 digits)
df_silver["card_number"] = df_silver["card_number"].astype(str).str[-4:].radd("**** **** **** ")

# iban mask
df_silver["iban"] = df_silver["iban"].astype(str).str[-4:].radd("AZ**********************")

# normalize strings
df_silver["currency"] = df_silver["currency"].str.upper()
df_silver["card_type"] = df_silver["card_type"].str.title()
df_silver["transaction_type"] = df_silver["transaction_type"].str.title()
df_silver["merchant_category"] = df_silver["merchant_category"].str.title()
df_silver["transaction_status"] = df_silver["transaction_status"].str.title()
df_silver["channel"] = df_silver["channel"].str.title()
df_silver["city"] = df_silver["city"].str.title()

# is_international to boolean
df_silver["is_international"] = df_silver["is_international"].astype(str).isin(["True", "true", "1"])

# Write back to MinIO/S3 (single CSV)
df_silver.to_csv(silver_path, index=False)

print(f"✅ Silver layer card_transactions yazıldı (masked): {silver_path}")
