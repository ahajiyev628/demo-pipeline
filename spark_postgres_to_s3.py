import argparse
from pyspark.sql import SparkSession

# Arguments
parser = argparse.ArgumentParser()
parser.add_argument("--postgres_url", required=True)
parser.add_argument("--postgres_table", required=True)
parser.add_argument("--postgres_user", required=True)
parser.add_argument("--postgres_password", required=True)
parser.add_argument("--output_path", required=True)
parser.add_argument("--input_path", required=True)

args = parser.parse_args()

# Spark session
spark = SparkSession.builder \
    .appName("PostgresToMinIO") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kubernetes.authenticate.driver.serviceAccountName", "airflow") \
    .getOrCreate()  # jars already baked in image

print(args.postgres_url)
print(args.postgres_table)
print(args.postgres_user)
print(args.postgres_password)
print(args.input_path)
print(args.output_path)
print("----------------------------------")

# # Read CSV from MinIO
# df_csv = spark.read.csv(args.input_path, header=True, inferSchema=True)
# df_csv.show(5)

# # Read Postgres table
# df_pg = spark.read \
#     .format("jdbc") \
#     .option("url", args.postgres_url) \
#     .option("dbtable", args.postgres_table) \
#     .option("user", args.postgres_user) \
#     .option("password", args.postgres_password) \
#     .load()

# df_pg.show(10)

# # Write to MinIO as Parquet
# df_pg.write \
#     .mode("overwrite") \
#     .parquet(args.output_path)

# spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql import Row


# Create a simple DataFrame
data = [Row(id=1, name="Alice"), Row(id=2, name="Bob"), Row(id=3, name="Charlie")]
df = spark.createDataFrame(data)

# Show the DataFrame
df.show()

# Stop Spark session
spark.stop()

