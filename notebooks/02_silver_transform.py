from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

BUCKET = "taxi-lakehouse-308946946086"

df = spark.read.format("delta").load(f"s3a://{BUCKET}/bronze/yellow_taxi")

df_silver = (
    df
    # Drop obviously bad records
    .filter(col("passenger_count") > 0)
    .filter(col("trip_distance") > 0)
    .filter(col("fare_amount") > 0)
    .filter(col("tpep_pickup_datetime") < col("tpep_dropoff_datetime"))
    # Type safety
    .withColumn("tpep_pickup_datetime", to_timestamp("tpep_pickup_datetime"))
    .withColumn("tpep_dropoff_datetime", to_timestamp("tpep_dropoff_datetime"))
    # Dedupe on natural key
    .dropDuplicates(["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID"])
    .select(
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "PULocationID", "DOLocationID",
        "fare_amount", "tip_amount", "total_amount", "payment_type"
    )
)

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(f"s3a://{BUCKET}/silver/yellow_taxi")
)

spark.sql("CREATE SCHEMA IF NOT EXISTS main.silver")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS main.silver.yellow_taxi
    USING DELTA
    LOCATION 's3a://{BUCKET}/silver/yellow_taxi'
""")

print(f"Silver load complete: {df_silver.count()} rows (from {df.count()} bronze rows)")