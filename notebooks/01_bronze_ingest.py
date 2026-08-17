from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

spark = SparkSession.builder.getOrCreate()

spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

BUCKET = "taxi-lakehouse-308946946086"
BRONZE_PATH = f"s3a://{BUCKET}/bronze/yellow_taxi"

import urllib.request

LOCAL_TMP = "/Volumes/main/default/tmp_landing/yellow_tripdata_2024-01.parquet"
CLOUDFRONT_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

urllib.request.urlretrieve(CLOUDFRONT_URL, LOCAL_TMP)

# COMMAND ----------

df_raw = spark.read.parquet(LOCAL_TMP)
df_raw.printSchema()

# COMMAND ----------

df_raw.count()

# COMMAND ----------

df_bronze = (
    df_raw
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
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