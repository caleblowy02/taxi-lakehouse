from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

spark = SparkSession.builder.getOrCreate()

BUCKET = "taxi-lakehouse-308946946086"
# SOURCE_PATH = "s3a://nyc-tlc/trip data/yellow_tripdata_2024-01.parquet"  # public NYC TLC bucket
BRONZE_PATH = f"s3a://{BUCKET}/bronze/yellow_taxi"

import urllib.request

LOCAL_TMP = "/tmp/yellow_tripdata_2024-01.parquet"
CLOUDFRONT_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

urllib.request.urlretrieve(CLOUDFRONT_URL, LOCAL_TMP)

df_raw = spark.read.parquet(f"file:{LOCAL_TMP}")

df_bronze = (
    df_raw
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .save(BRONZE_PATH)
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS bronze.yellow_taxi
    USING DELTA
    LOCATION '{BRONZE_PATH}'
""")

print(f"Bronze load complete: {df_bronze.count()} rows")