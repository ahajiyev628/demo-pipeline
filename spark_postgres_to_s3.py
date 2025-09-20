import argparse
from pyspark.sql import SparkSession
import subprocess

# Arguments
parser = argparse.ArgumentParser()
parser.add_argument("--postgres_url", required=True)
parser.add_argument("--postgres_table", required=True)
parser.add_argument("--postgres_user", required=True)
parser.add_argument("--postgres_password", required=True)
parser.add_argument("--output_path", required=True)
parser.add_argument("--input_path", required=True)

args = parser.parse_args()

driver_host = check_output(
    "grep -E '^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+' /etc/hosts | grep -v '127.0.0.1' | awk '{print $1}'",
    shell=True,
    text=True
).strip()

# Spark session
spark = (
    SparkSession.builder
    .appName("PostgresToMinIO")
    # MinIO S3A configs
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio-external.default.svc.cluster.local:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.access.key", "adminic")
    .config("spark.hadoop.fs.s3a.secret.key", "adminic123")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    # Other useful configs
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.kubernetes.driver.service.expose", "true")
    .config("spark.kubernetes.container.image", "ahajiyev/spark-minio6:3.5.1")
    .config("spark.kubernetes.authenticate.driver.serviceAccountName", "airflow")
    .config("spark.driver.host", driver_host) 
    .config("spark.executor.instances", "1")
    .config("spark.executor.cores", "1")
    .config("spark.executor.memory", "3g")
    .config("spark.driver.memory", "3g")

    .getOrCreate()
)

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
rdd = spark.sparkContext.parallelize(data, numSlices=1)
df = spark.createDataFrame(rdd, ["col1", "col2"])


# Show the DataFrame
df.show()

