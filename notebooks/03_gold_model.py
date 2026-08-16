# 03_gold_model.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, year, month, dayofmonth, dayofweek

spark = SparkSession.builder.getOrCreate()
BUCKET = "taxi-lakehouse-<your-account-id>"

silver = spark.read.format("delta").load(f"s3a://{BUCKET}/silver/yellow_taxi")

# --- Dimension: Date ---
dim_date = (
    silver.select(col("tpep_pickup_datetime").alias("date")).distinct()
    .withColumn("date_key", monotonically_increasing_id())
    .withColumn("year", year("date"))
    .withColumn("month", month("date"))
    .withColumn("day", dayofmonth("date"))
    .withColumn("day_of_week", dayofweek("date"))
)
dim_date.write.format("delta").mode("overwrite").save(f"s3a://{BUCKET}/gold/dim_date")

# --- Dimension: Location ---
dim_location = (
    silver.select(col("PULocationID").alias("location_id")).distinct()
    .withColumn("location_key", monotonically_increasing_id())
)
dim_location.write.format("delta").mode("overwrite").save(f"s3a://{BUCKET}/gold/dim_location")

# --- Dimension: Vendor ---
dim_vendor = (
    silver.select(col("VendorID").alias("vendor_id")).distinct()
    .withColumn("vendor_key", monotonically_increasing_id())
)
dim_vendor.write.format("delta").mode("overwrite").save(f"s3a://{BUCKET}/gold/dim_vendor")

# --- Fact: Trips ---
fact_trips = (
    silver
    .join(dim_date, silver.tpep_pickup_datetime == dim_date.date, "left")
    .join(dim_location, silver.PULocationID == dim_location.location_id, "left")
    .join(dim_vendor, silver.VendorID == dim_vendor.vendor_id, "left")
    .select(
        "date_key", "location_key", "vendor_key",
        "passenger_count", "trip_distance",
        "fare_amount", "tip_amount", "total_amount", "payment_type"
    )
)
fact_trips.write.format("delta").mode("overwrite").save(f"s3a://{BUCKET}/gold/fact_trips")

for tbl, path in [("dim_date", "dim_date"), ("dim_location", "dim_location"),
                   ("dim_vendor", "dim_vendor"), ("fact_trips", "fact_trips")]:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold.{tbl}
        USING DELTA LOCATION 's3a://{BUCKET}/gold/{path}'
    """)

print("Gold star schema built: 1 fact table, 3 dimensions")