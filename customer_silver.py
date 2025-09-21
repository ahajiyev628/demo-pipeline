# customer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import unidecode

# Spark session
spark = (
    SparkSession.builder
    .appName("SilverLayerCustomer")
    .getOrCreate()
)

# Paths
bronze_path = "./data/customers.csv"
silver_path = "./data/silver/customer.csv"

# Schema
customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("full_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birth_date", StringType(), True),
    StructField("customer_fin", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    # StructField("city", StringType(), True),
    StructField("registration_date", StringType(), True),
    StructField("marital_status", StringType(), True),
    StructField("education_level", StringType(), True),
    StructField("customer_segment", StringType(), True)
])

# Bronze oxu
df_bronze = spark.read.csv(
    bronze_path,
    header=True,
    schema=customer_schema
)

char_map = {
    "ə": "e",
    "Ə": "E",
    "ı": "i",
    "İ": "I",
    "ş": "s",
    "Ş": "S",
    "ç": "c",
    "Ç": "C",
    "ö": "o",
    "Ö": "O",
    "ğ": "g",
    "Ğ": "G",
    "ü": "u",
    "Ü": "U"
}

def normalize_text(x: str):
    if not x:
        return x
    for k, v in char_map.items():
        x = x.replace(k, v)
    return unidecode.unidecode(x)

from pyspark.sql.functions import udf
normalize_udf = udf(normalize_text, StringType())

# Telefon cleaning fonksiyonu
def clean_phone(phone: str):
    if not phone:
        return None
    import re
    digits = re.sub(r"\D", "", phone)  # sadece rakamlar
    if digits.startswith("994"):
        return f"+{digits}"
    elif digits.startswith("0"):
        return f"+994{digits[1:]}"
    else:
        return f"+994{digits}"
    
clean_phone_udf = udf(clean_phone, StringType())

# Silver təmizləmə
df_silver = (
    df_bronze
    # full_name split
    .withColumn("first_name", normalize_udf(F.split(F.col("full_name"), " ").getItem(0)))
    .withColumn("last_name", normalize_udf(F.split(F.col("full_name"), " ").getItem(1)))
    
    # tarix 
    .withColumn("birth_date", F.to_date("birth_date", "yyyy-MM-dd"))
    .withColumn("registration_date", F.to_date("registration_date", "yyyy-MM-dd"))
    
    # gender 
    .withColumn("gender", 
        F.when(F.lower(F.col("gender")).startswith("m"), "Male")
         .when(F.lower(F.col("gender")).startswith("f"), "Female")
         .otherwise("Unknown")
    )
    
    # marital status normalize
    .withColumn("marital_status",
        F.when(F.col("marital_status").isin("Single", "Married"), F.col("marital_status"))
         .otherwise("Unknown")
    )
    
    # city normalize
    # .withColumn("city", normalize_udf(F.initcap(F.col("city"))))
    
    # email lowercase
    .withColumn("email", F.lower(F.col("email")))
    
    # phone clean
    .withColumn("phone", clean_phone_udf(F.col("phone")))
)

(
    df_silver
    .coalesce(1)  
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(silver_path)
)

print(f"✅ Silver layer customer yazıldı: {silver_path}")
