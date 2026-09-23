from pyspark.sql import SparkSession
from transformations import aggregate_hourly
from delta.tables import DeltaTable


SILVER_PATH = "/opt/spark-data/delta/weather_silver"
GOLD_PATH = "/opt/spark-data/delta/weather_gold"

spark = SparkSession.builder.appName("WeatherGold").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "Africa/Tunis")

try:
    # 1. Read clean Silver data.
    silver_df = spark.read.format("delta").load(SILVER_PATH)

    # 2. Aggregate by hour and location.
    gold_df = aggregate_hourly(silver_df)

    # 3. Update existing rows only when business metrics change.
    if DeltaTable.isDeltaTable(spark, GOLD_PATH):
        gold_table = DeltaTable.forPath(spark, GOLD_PATH)
        (
            gold_table.alias("target")
            .merge(
                gold_df.alias("source"),
                """
                target.hour = source.hour
                AND target.latitude = source.latitude
                AND target.longitude = source.longitude
                """,
            )
            .whenMatchedUpdateAll(condition="""
                NOT (
                    target.avg_temperature <=> source.avg_temperature
                    AND target.min_temperature <=> source.min_temperature
                    AND target.max_temperature <=> source.max_temperature
                    AND target.avg_humidity <=> source.avg_humidity
                    AND target.avg_wind_speed <=> source.avg_wind_speed
                    AND target.max_wind_speed <=> source.max_wind_speed
                    AND target.observation_count <=> source.observation_count
                )
            """)
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        gold_df.write.format("delta").mode("overwrite").save(GOLD_PATH)

    print("Gold aggregation completed")
    (
        spark.read.format("delta")
        .load(GOLD_PATH)
        .orderBy("hour")
        .show(truncate=False)
    )
finally:
    spark.stop()
