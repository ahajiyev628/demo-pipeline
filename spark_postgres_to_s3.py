import argparse
from pyspark.sql import SparkSession

# Arguments
parser = argparse.ArgumentParser()
parser.add_argument("--postgres_url", required=True)
parser.add_argument("--postgres_table", required=True)
parser.add_argument("--postgres_user", required=True)
parser.add_argument("--postgres_password", required=True)
parser.add_argument("--output_path", required=True)
args = parser.parse_args()

def get_driver_host_from_hosts():
    try:
        with open('/etc/hosts', 'r') as f:
            for line in f:
                parts = line.split()
                return parts[0]
    except FileNotFoundError:
        return "Could not read /etc/hosts"

# Spark session
spark = SparkSession.builder \
    .appName("PostgresToMinIO") \
    .config("spark.driver.host", get_driver_host_from_hosts()) \
    .getOrCreate()  # jars already pre-baked in image

print(args.postgres_url)
print(args.postgres_table)
print(args.postgres_user)
print(args.postgres_password)

df = spark.read.parquet(args.output_path)
df.show(5)

# Postgres JDBC read
df = spark.read \
    .format("jdbc") \
    .option("url", args.postgres_url) \
    .option("dbtable", args.postgres_table) \
    .option("user", args.postgres_user) \
    .option("password", args.postgres_password) \
    .load()

df.limit(10).show()

# Write to MinIO (S3)
df.write \
    .mode("overwrite") \
    .parquet(args.output_path)

spark.stop()
