from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

BRONZE_PATH = "/opt/spark-data/delta/weather_bronze"
SILVER_PATH = "/opt/spark-data/delta/weather_silver"

spark = SparkSession.builder.appName("CheckSilver").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

try:
    bronze = spark.read.format("delta").load(BRONZE_PATH)
    silver = spark.read.format("delta").load(SILVER_PATH)

    print("=== COUNTS ===")
    print(f"Bronze rows : {bronze.count()}")
    print(f"Silver rows : {silver.count()}")

    print("\n=== SILVER DATA ===")
    silver.orderBy("weather_time").show(truncate=False)

    print("\n=== DUPLICATE BUSINESS KEYS ===")
    (
        silver.groupBy("latitude", "longitude", "weather_time")
        .agg(count("*").alias("count"))
        .filter(col("count") > 1)
        .show(truncate=False)
    )

    print("\n=== SCHEMA ===")
    silver.printSchema()
finally:
    spark.stop()
