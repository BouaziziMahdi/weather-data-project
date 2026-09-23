import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format


GOLD_PATH = "/opt/spark-data/delta/weather_gold"
DB_HOST = os.environ["WEATHER_DB_HOST"]
DB_PORT = os.environ["WEATHER_DB_PORT"]
DB_NAME = os.environ["WEATHER_DB_NAME"]
DB_USER = os.environ["WEATHER_DB_USER"]
DB_PASSWORD = os.environ["WEATHER_DB_PASSWORD"]
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

UPSERT_SQL = """
INSERT INTO weather_hourly (
    hour, latitude, longitude,
    avg_temperature, min_temperature, max_temperature,
    avg_humidity, avg_wind_speed, max_wind_speed,
    observation_count, updated_at
)
SELECT
    CAST(hour AS TIMESTAMP), latitude, longitude,
    avg_temperature, min_temperature, max_temperature,
    avg_humidity, avg_wind_speed, max_wind_speed,
    observation_count, updated_at
FROM weather_hourly_staging
ON CONFLICT (hour, latitude, longitude)
DO UPDATE SET
    avg_temperature = EXCLUDED.avg_temperature,
    min_temperature = EXCLUDED.min_temperature,
    max_temperature = EXCLUDED.max_temperature,
    avg_humidity = EXCLUDED.avg_humidity,
    avg_wind_speed = EXCLUDED.avg_wind_speed,
    max_wind_speed = EXCLUDED.max_wind_speed,
    observation_count = EXCLUDED.observation_count,
    updated_at = EXCLUDED.updated_at
WHERE weather_hourly.avg_temperature IS DISTINCT FROM EXCLUDED.avg_temperature
   OR weather_hourly.min_temperature IS DISTINCT FROM EXCLUDED.min_temperature
   OR weather_hourly.max_temperature IS DISTINCT FROM EXCLUDED.max_temperature
   OR weather_hourly.avg_humidity IS DISTINCT FROM EXCLUDED.avg_humidity
   OR weather_hourly.avg_wind_speed IS DISTINCT FROM EXCLUDED.avg_wind_speed
   OR weather_hourly.max_wind_speed IS DISTINCT FROM EXCLUDED.max_wind_speed
   OR weather_hourly.observation_count IS DISTINCT FROM EXCLUDED.observation_count
"""

spark = SparkSession.builder.appName("GoldToPostgres").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "Africa/Tunis")

try:
    gold_df = (
        spark.read.format("delta").load(GOLD_PATH)
        # Explicit local wall-clock text avoids JDBC/JVM timezone conversion
        # for the serving TIMESTAMP; updated_at remains a timestamp instant.
        .withColumn("hour", date_format("hour", "yyyy-MM-dd HH:mm:ss"))
    )

    # Shared staging table: run only one instance of this job at a time.
    (
        gold_df.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "weather_hourly_staging")
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    jvm = spark.sparkContext._gateway.jvm
    connection = jvm.java.sql.DriverManager.getConnection(
        JDBC_URL, DB_USER, DB_PASSWORD
    )
    try:
        connection.setAutoCommit(False)
        statement = connection.createStatement()
        try:
            affected_rows = statement.executeUpdate(UPSERT_SQL)
            connection.commit()
            print(f"PostgreSQL UPSERT completed: {affected_rows} row(s) affected")
        except Exception:
            connection.rollback()
            raise
        finally:
            statement.close()
    finally:
        connection.close()
finally:
    spark.stop()
