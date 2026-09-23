"""Pure Spark transformations shared by jobs and CI tests (no I/O on import)."""
from pyspark.sql.functions import (
    col, from_json, to_timestamp, row_number, avg, min, max, count,
    date_trunc, current_timestamp,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.window import Window

current_schema = StructType([
    StructField("time", StringType(), True),
    StructField("interval", IntegerType(), True),
    StructField("temperature_2m", DoubleType(), True),
    StructField("relative_humidity_2m", IntegerType(), True),
    StructField("wind_speed_10m", DoubleType(), True),
    StructField("weather_code", IntegerType(), True)
])

weather_schema = StructType([
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timezone", StringType(), True),
    StructField("elevation", DoubleType(), True),
    StructField("current", current_schema, True),
])


def clean_weather(bronze_df):
    parsed = bronze_df.withColumn(
        "weather",
        from_json("raw_json", weather_schema)
    )
    silver_df = parsed.select(
        col("weather.latitude").alias("latitude"),
        col("weather.longitude").alias("longitude"),
        col("weather.timezone").alias("timezone"),
        col("weather.elevation").alias("elevation"),

        to_timestamp(
            col("weather.current.time")
        ).alias("weather_time"),

        col("weather.current.temperature_2m")
            .alias("temperature_2m"),

        col("weather.current.relative_humidity_2m")
            .alias("relative_humidity_2m"),

        col("weather.current.wind_speed_10m")
            .alias("wind_speed_10m"),

        col("weather.current.weather_code")
            .alias("weather_code"),

        col("topic"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp"),
    )

    valid_df = silver_df.filter(
        (col("latitude").isNotNull()) &
        (col("longitude").isNotNull()) &
        (col("timezone").isNotNull()) &
        (col("elevation").isNotNull()) &
        (col("weather_time").isNotNull()) &
        (col("temperature_2m").isNotNull()) &
        (col("relative_humidity_2m").between(0, 100)) &
        (col("wind_speed_10m") >= 0)
    )
    return valid_df


def latest_observations(batch_df):
    # Keep the latest Kafka event for each business key.
    window = Window.partitionBy(
        "latitude", "longitude", "weather_time"
    ).orderBy(col("kafka_timestamp").desc(), col("offset").desc())
    clean_batch = (
        batch_df
        .withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    return clean_batch


def aggregate_hourly(silver_df):
    gold_df = (
        silver_df
        .withColumn("hour", date_trunc("hour", col("weather_time")))
        .groupBy("hour", "latitude", "longitude")
        .agg(
            avg("temperature_2m").alias("avg_temperature"),
            min("temperature_2m").alias("min_temperature"),
            max("temperature_2m").alias("max_temperature"),
            avg("relative_humidity_2m").alias("avg_humidity"),
            avg("wind_speed_10m").alias("avg_wind_speed"),
            max("wind_speed_10m").alias("max_wind_speed"),
            count("*").alias("observation_count"),
        )
        .withColumn("updated_at", current_timestamp())
    )

    return gold_df
