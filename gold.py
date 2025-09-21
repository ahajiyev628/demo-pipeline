from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("GoldLayerWithCurrency").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

silver_customer_path = "./data/silver/customer.csv"
silver_txn_path = "./data/silver/card_transactions"
gold_path = "./data/gold/"

# Conversion rates
conversion_rates = {
    'AZN': 1.0,
    'USD': 1.7,
    'EUR': 1.85
}

# ---- Silver Tables ----
df_customer = spark.read.csv(silver_customer_path, header=True, inferSchema=True)
df_txn = spark.read.csv(silver_txn_path, header=True, inferSchema=True)

# ---- dim_currency ----
currency_data = [(code, rate) for code, rate in conversion_rates.items()]
df_currency = spark.createDataFrame(currency_data, ["currency_code", "conversion_rate"])

# ---- dim_date ----
df_date = df_txn.select(F.col("transaction_datetime").alias("date")).distinct() \
    .withColumn("year", F.year("date")) \
    .withColumn("month", F.month("date")) \
    .withColumn("day", F.dayofmonth("date")) \
    .withColumn("weekday", F.date_format("date","E")) \
    .withColumn("is_weekend", F.when(F.date_format("date","u").isin("6","7"), True).otherwise(False))

# ---- dim_card ----
df_card = df_txn.select("card_number","card_type","iban").distinct()

# ---- dim_merchant ----
df_merchant = df_txn.select("merchant_category","city","channel","is_international").distinct()

# ---- fact_card_transaction ----
df_fact = df_txn \
    .join(df_customer, "customer_id", "left") \
    .join(df_card, ["card_number","card_type","iban"], "left") \
    .join(df_merchant, ["merchant_category","city","channel","is_international"], "left") \
    .join(df_date, df_txn.transaction_datetime == df_date.date, "left") \
    .join(df_currency, df_txn.currency == df_currency.currency_code, "left") \
    .withColumn("azn_ekv", F.col("transaction_amount") * F.col("conversion_rate")) \
    .select(
        "transaction_id",
        "transaction_datetime",
        "customer_id",
        "card_number",
        "card_type",
        "iban",
        "merchant_category",
        "city",
        "channel",
        "is_international",
        F.col("currency").alias("currency_code"),
        "transaction_amount",
        "azn_ekv",
        "transaction_type",
        "transaction_status"
    )
df_currency.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{gold_path}/dim_currency")
df_date.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{gold_path}/dim_date")
df_card.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{gold_path}/dim_card")
df_customer.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{gold_path}/dim_customer")
df_merchant.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{gold_path}/dim_merchant")
df_fact.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{gold_path}/fact_card_transaction")

