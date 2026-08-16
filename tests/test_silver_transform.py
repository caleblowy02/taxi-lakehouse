# tests/test_silver_transform.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()

def test_filters_negative_fares(spark):
    df = spark.createDataFrame(
        [(1, 10.0, -5.0), (2, 5.0, 12.0)],
        ["passenger_count", "trip_distance", "fare_amount"]
    )
    filtered = df.filter(df.fare_amount > 0)
    assert filtered.count() == 1