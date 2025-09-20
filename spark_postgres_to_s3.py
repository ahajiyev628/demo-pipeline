import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--postgres_url", required=True)
parser.add_argument("--postgres_table", required=True)
parser.add_argument("--postgres_user", required=True)
parser.add_argument("--postgres_password", required=True)
parser.add_argument("--output_path", required=True)
args = parser.parse_args()

spark = SparkSession.builder \
    .appName("PostgresToMinIO") \
    .getOrCreate()

jdbc_url = args.postgres_url
print("Allahverdi")
pg_table = args.postgres_table
pg_user = args.postgres_user
pg_password = args.postgres_password

df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", pg_table) \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .load()

df.show(5)

output_path = args.output_path

df.write \
    .mode("overwrite") \
    .parquet(output_path)

spark.stop()
