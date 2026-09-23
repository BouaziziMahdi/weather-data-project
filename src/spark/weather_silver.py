
from pyspark.sql import SparkSession
from transformations import clean_weather, latest_observations
from delta.tables import DeltaTable
import os
BRONZE_PATH="/opt/spark-data/delta/weather_bronze"
SILVER_PATH="/opt/spark-data/delta/weather_silver"
CHECKPOINT_PATH = os.getenv(
    "SILVER_CHECKPOINT_PATH", "/opt/spark-data/checkpoints/weather_silver"
)
RUN_MODE = os.getenv("SILVER_RUN_MODE", "continuous")
if RUN_MODE not in {"continuous", "available-now"}:
    raise ValueError("SILVER_RUN_MODE must be continuous or available-now")
spark=(
    SparkSession.builder
    .appName("WeatherSilver")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

spark.conf.set(
    "spark.sql.session.timeZone", "Africa/Tunis"
)

read_bronze = spark.readStream.format("delta").load(BRONZE_PATH)
valid_df = clean_weather(read_bronze)

def upsert_silver(micro_batch_df, batch_id):

    if micro_batch_df.isEmpty():
        return

    clean_batch = latest_observations(micro_batch_df)

    if DeltaTable.isDeltaTable(
        spark,
        SILVER_PATH
    ):

        silver_table = DeltaTable.forPath(
            spark,
            SILVER_PATH
        )

        (
            silver_table.alias("target")
            .merge(
                clean_batch.alias("source"),
                """
                target.latitude = source.latitude
                AND target.longitude = source.longitude
                AND target.weather_time = source.weather_time
                """
            )
            .whenMatchedUpdateAll(condition="""
                source.kafka_timestamp > target.kafka_timestamp
                OR (
                    source.kafka_timestamp = target.kafka_timestamp
                    AND source.offset > target.offset
                )
            """)
            .whenNotMatchedInsertAll()
            .execute()
        )

    else:
        (
            clean_batch.write
            .format("delta")
            .mode("overwrite")
            .save(SILVER_PATH)
        )


writer = (
    valid_df.writeStream
    .foreachBatch(upsert_silver)
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )
)

if RUN_MODE == "available-now":
    writer = writer.trigger(availableNow=True)

query = writer.start()
query.awaitTermination()
spark.stop()
