import os

from pyspark.sql import SparkSession

RUN_MODE = os.getenv("BRONZE_RUN_MODE", "continuous")
if RUN_MODE not in {"continuous", "available-now"}:
    raise ValueError("BRONZE_RUN_MODE must be continuous or available-now")


spark = (
    SparkSession.builder
    .appName("WeatherBronze")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "weather")
    .option("startingOffsets", "earliest")
    .load()
)


bronze_df = raw_stream.selectExpr(
    "CAST(key AS STRING) AS kafka_key",
    "CAST(value AS STRING) AS raw_json",
    "topic",
    "partition",
    "offset",
    "timestamp AS kafka_timestamp"
)


writer = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/opt/spark-data/checkpoints/weather_bronze"
    )
)

if RUN_MODE == "available-now":
    writer = writer.trigger(availableNow=True)

query = writer.start("/opt/spark-data/delta/weather_bronze")
query.awaitTermination()
spark.stop()
